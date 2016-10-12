import json
import logging
import os
import socket
import time
import uuid

import pika
import isodate
import utc
import mettle_protocol.messages as mp


SLEEP_INTERVAL_ON_RABBITMQ_EXCEPTION = 5


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PipelineNack(Exception):
    """
    Services can raise this exception from the get_targets method to refuse to
    run a pipeline.

    You must provide a message explaining the reason for the nack (e.g. 'Stale
    run', or 'Preconditions not met').

    You may optionally provide a reannounce_time datetime (with UTC tzinfo).  If
    you do, then the pipeline will be reannounced after that time.
    """

    def __init__(self, message, reannounce_time=None, **kwargs):
        self.message = message
        self.reannounce_time = reannounce_time
        super(PipelineNack, self).__init__(message, **kwargs)


class RabbitChannel(object):
    """
    Wrapper class for a Pika channel (and underlying connection).

    This class performs some __getattr__ magic to recover from closed
    Pika connections.
    """

    def __init__(self, rabbit_url, service_name, pipelines,
                 queue_name):
        self.conn = None
        self.chan = None
        self.service_name = service_name
        self.pipelines = pipelines
        self.queue_name = queue_name
        self._conn_parameters = pika.connection.URLParameters(rabbit_url)
        self._establish_connection()

    def _establish_connection(self):
        self.conn = pika.BlockingConnection(self._conn_parameters)
        self.chan = self.conn.channel()
        self.chan.basic_qos(prefetch_count=0)

        # Declare the mettle protocol exchanges.
        mp.declare_exchanges(self.chan)

        # Announce the service.  The dispatcher will hear this message and record
        # the service and pipeline names in the DB so they'll appear in the UI.
        mp.announce_service(self.chan,
                            self.service_name,
                            list(self.pipelines.keys()))

        self.chan.queue_declare(queue=self.queue_name,
                                exclusive=False, durable=True)

        for name in self.pipelines:
            routing_key = mp.pipeline_routing_key(self.service_name, name)
            self.chan.queue_bind(exchange=mp.ANNOUNCE_PIPELINE_RUN_EXCHANGE,
                                 queue=self.queue_name, routing_key=routing_key)

    def __getattr__(self, name):
        retry_methods = (
            'basic_publish',
            'basic_qos',
            'exchange_declare',
            'flow'
        )

        attr = getattr(self.chan, name)

        if callable(attr) and attr in retry_methods:
            # Wrap this method in a function that is able to recover from
            # Pika exceptions by re-establishing the channel (and underlying
            # connection).
            def _callable(*args, **kwargs):
                try:
                    return attr(*args, **kwargs)
                except (pika.exceptions.AMQPError, AttributeError) as e:
                    if isinstance(e, AttributeError) and \
                       "'NoneType' object has no attribute 'sendall'" not in str(e):
                        raise

                    # Argg! Most likely the connection was dropped. This
                    # usually happens for long-running procs.

                    logger.info('Pika Error: %s' % e)

                    # Clean up the (possibly dangling) conn and chan objects.
                    if self.chan:
                        try:
                            self.chan.close()
                        except pika.exceptions.ChannelClosed:
                            pass
                    if self.conn:
                        try:
                            self.conn.close()
                        except pika.exceptions.ConnectionClosed:
                            pass

                    # Re-establish the connection.
                    self._establish_connection()

                    # Now, try the method again. This time it should work fine.
                    __callable = getattr(self.chan, name)
                    return __callable(*args, **kwargs)

            return _callable

        return attr


class Pipeline(object):
    assignment_wait_secs = 30

    def __init__(self, conn, chan, service_name, pipeline_name, run_id=None,
                 target=None, job_id=None):
        self.log_line_num = 0
        self.conn = conn
        self.chan = chan
        self.service_name = service_name
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self.target = target
        self.job_id = job_id
        self.queue = get_worker_name()
        self._claim_response = None

    def _claim_job(self, target_time, target):
        """
        Make a an RPC call to the dispatcher to claim self.job_id.

        If claiming the job_id is successful, the dispatcher will return '1',
        and this function will return True.

        If claiming the job is unsuccessful, probably because some other worker
        has already claimed it, the dispatcher will return '0', and this
        function will return False.
        """
        self.corr_id = str(uuid.uuid4())
        logger.info('Claiming job %s.' % self.job_id)
        self.chan.queue_declare(queue=self.queue, exclusive=True)

        start_time = utc.now()
        expire_time = self.get_expire_time(target_time, target, start_time)

        consumer_tag = self.chan.basic_consume(self._on_claim_response,
                                               no_ack=True, queue=self.queue)
        try:
            mp.claim_job(self.chan, self.job_id, self.queue,
                         start_time.isoformat(),
                         expire_time.isoformat(), self.corr_id)

            # Block while we wait for a response, as in the RabbitMQ RPC example
            # doc.
            wait_start = utc.now()
            while self._claim_response == None:
                self.conn.process_data_events()
                elapsed = (utc.now() - wait_start).total_seconds()
                if elapsed > self.assignment_wait_secs:
                    logger.warning('Timed out waiting for job grant %s.' %
                                   self.job_id)
                    return False

            granted = self._claim_response == u'1'
            if granted:
                logger.info('Claimed job %s.' % self.job_id)
            else:
                logger.info('Failed to claim job %s.' % self.job_id)
        finally:
            self.chan.basic_cancel(consumer_tag)
        return granted

    def _on_claim_response(self, ch, method, props, body):
        if props.correlation_id == self.corr_id:
            self._claim_response = body.decode('utf-8')
        else:
            logger.warning('corr_id mismatch.  mine: %s\nreceived: %s' %
                           (self.corr_id, props.correlation_id))

    def log(self, msg):
        if self.run_id is None:
            raise ValueError("Must set run_id to enable job logging.")
        elif self.target is None:
            raise ValueError("Must set target to enable job logging.")
        elif self.job_id is None:
            raise ValueError("Must set job_id to enable job logging.")
        mp.send_log_msg(self.chan, self.service_name, self.pipeline_name,
                        self.run_id, self.target, self.job_id, self.log_line_num,
                        msg)
        self.log_line_num += 1

    def get_targets(self, target_time):
        """ Subclasses should implement this method.
        """
        raise NotImplementedError

    def get_expire_time(self, target_time, target, start_time):
        """ Subclasses should implement this method.
        """
        raise NotImplementedError

    def make_target(self, target_time, target, parameters=None):
        """ Subclasses should implement this method.
        """
        raise NotImplementedError

    def get_target_parameters(self, target_time):
        """
        Return a dict where the keys are targets, and the values are
        dictionaries of key/val pairs that will be used for routing job
        announcements and passed in job announcement payloads.  Example:

        {
            "target1": {"foo": "bar", "baz": "quux"},
            "target2": {"blah": "blerg"},
        }

        If a target needs no parameters, you may omit it.

        If a pipeline needs no parameters for any of its targets, it need not
        implement this method.
        """
        return {}


# Do this outside the function so it will be consistent across invocations.
# Otherwise we make a ton of one-off queues when claiming jobs.
random_worker_id = str(uuid.uuid4())
def get_worker_name():
    """
    Returns a string name for this instance that should uniquely identify it
    across a datacenter.
    """
    # NOTE the pid here is not really unique per machine.  If you're running in
    # a container, then each container has an isolated PID namespace, so you
    # could have a lot of processes on the host that think they're PID 1 or PID
    # 2.
    hostname = socket.getfqdn()
    pid = str(os.getpid())
    return '_'.join([
        hostname,
        pid,
        random_worker_id
    ])


def run_pipelines(service_name, rabbit_url, pipelines, queue_name=None):
    while True:
        try:
            # Expects 'pipelines' to be a dict of pipeline names (as keys) and
            # classes (as values),

            queue_name = queue_name or mp.service_queue_name(service_name)
            rabbit = RabbitChannel(rabbit_url, service_name, pipelines,
                                   queue_name)

            for method, properties, body in rabbit.consume(queue=queue_name):
                data = json.loads(body.decode('utf-8'))
                pipeline_name = data['pipeline']
                pipeline_cls = pipelines[pipeline_name]
                target_time = isodate.parse_datetime(data['target_time'])
                run_id = data['run_id']

                if method.exchange == mp.ANNOUNCE_PIPELINE_RUN_EXCHANGE:
                    pipeline = pipeline_cls(rabbit.conn, rabbit, service_name,
                                            pipeline_name, run_id)
                    # If it's a pipeline run announcement, then call get_targets
                    # and publish result.
                    try:
                        targets = pipeline.get_targets(target_time)
                        target_params = pipeline.get_target_parameters(target_time)
                        logger.info("Acking pipeline run %s:%s:%s" % (service_name,
                                                                      data['pipeline'],
                                                                      data['run_id']))
                        mp.ack_pipeline_run(rabbit, service_name, data['pipeline'],
                                            data['target_time'], run_id,
                                            targets, target_params)
                    except PipelineNack as pn:
                        logger.info("Nacking pipeline run %s:%s:%s" % (service_name,
                                                                      data['pipeline'],
                                                                      data['run_id']))
                        reannounce_time = None
                        if pn.reannounce_time:
                            reannounce_time = pn.reannounce_time.isoformat()
                        mp.nack_pipeline_run(rabbit, service_name, data['pipeline'], run_id,
                              reannounce_time, pn.message)
                elif method.exchange == '' and method.routing_key == queue_name:
                    # Job message published directly to our queue, not going
                    # through an exchange.
                    job_id = data['job_id']
                    target = data['target']
                    pipeline = pipeline_cls(rabbit.conn, rabbit, service_name,
                                            pipeline_name, run_id, target,
                                            job_id)
                    # If it's a job announcement, then publish ack, run job,
                    # then publish completion.
                    # publish ack
                    claimed = pipeline._claim_job(target_time, data['target'])

                    if claimed:
                        # WOOO!  Actually do some work here.
                        succeeded = pipeline.make_target(target_time, target,
                                                         data['target_parameters'])

                        mp.end_job(rabbit, service_name, data['pipeline'],
                                   data['target_time'], data['target'],
                                   job_id, utc.now().isoformat(), succeeded)
                    else:
                        logger.info('Failed to claim job %s.' % job_id)
                rabbit.basic_ack(method.delivery_tag)

        except (pika.exceptions.AMQPError, AttributeError) as e:
            if isinstance(e, AttributeError) and \
               "'NoneType' object has no attribute 'sendall'" not in str(e):
                raise

            logger.exception('Unexpected RabbitMQ exception: %s.' % str(e))
            logger.info('Connection will be re-established in %s seconds!'
                        % SLEEP_INTERVAL_ON_RABBITMQ_EXCEPTION)
            time.sleep(SLEEP_INTERVAL_ON_RABBITMQ_EXCEPTION)
