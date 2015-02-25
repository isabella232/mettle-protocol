import json
import logging
import os
import random
import socket
import string
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


class RabbitChannel(object):
    """
    Wrapper class for a Pika channel (and underlying connection).

    This class performs some __getattr__ magic to recover from closed
    Pika connections.
    """

    def __init__(self, rabbit_url, service_name, pipelines):
        self.conn = None
        self.chan = None
        self.service_name = service_name
        self.pipelines = pipelines
        self._conn_parameters = pika.connection.URLParameters(rabbit_url)
        self._establish_connection()

    def _establish_connection(self):
        self.conn = pika.BlockingConnection(self._conn_parameters)
        self.chan = self.conn.channel()
        self.chan.basic_qos(prefetch_count=0)

        # Declare the mettle protocol exchanges.
        mp.declare_exchanges(self.chan)

        # Declare the queue shared by all instances of this worker.
        shared_queue = 'etl_service_' + self.service_name
        self.chan.queue_declare(queue=shared_queue,
                                exclusive=False, durable=True)

        for name in self.pipelines:
            routing_key = mp.pipeline_routing_key(self.service_name, name)
            self.chan.queue_bind(exchange=mp.ANNOUNCE_PIPELINE_RUN_EXCHANGE,
                                 queue=shared_queue, routing_key=routing_key)
            self.chan.queue_bind(exchange=mp.ANNOUNCE_JOB_EXCHANGE,
                                 queue=shared_queue, routing_key=routing_key)

    def __getattr__(self, name):
        attr = getattr(self.chan, name)

        if callable(attr):
            # This is a (Pika) channel callable object. Wrap it in a function
            # that is able to recover from Pika exceptions by re-establishing
            # the channel (and underlying connection).
            def _callable(*args, **kwargs):
                try:
                    return attr(*args, **kwargs)
                except (pika.exceptions.AMQPError, AttributeError) as e:
                    # Argg! Most likely the connection was dropped. This
                    # usually happens for long-running procs.
                    if isinstance(e, AttributeError) and \
                       str(e) != "'NoneType' object has no attribute 'sendall'":
                        raise

                    # Re-establish the connection.
                    self._establish_connection()

                    # Now, try the callable again. This time it should work fine.
                    __callable = getattr(self.chan, name)
                    return __callable(*args, **kwargs)

            return _callable

        # Normal attribute.
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

            granted = self._claim_response == '1'
            if granted:
                logger.info('Claimed job %s.' % self.job_id)
            else:
                logger.info('Failed to claim job %s.' % self.job_id)
        finally:
            self.chan.basic_cancel(consumer_tag)
        return granted

    def _on_claim_response(self, ch, method, props, body):
        if props.correlation_id == self.corr_id:
            self._claim_response = body
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

    def make_target(self, target_time, target):
        """ Subclasses should implement this method.
        """
        raise NotImplementedError


def get_worker_name():
    """
    Returns a string name for this instance that should uniquely identify it
    across a datacenter.
    """
    hostname = socket.getfqdn()
    pid = str(os.getpid())
    random_bit = ''.join(
        [random.choice(string.ascii_lowercase) for x in xrange(8)])
    return '_'.join([
        hostname,
        pid,
        random_bit
    ])


def run_pipelines(service_name, rabbit_url, pipelines):
    while True:
        try:
            # Expects 'pipelines' to be a dict of pipeline names and instances,
            # where the key for each entry is a tuple of
            # (service_name, pipeline_name)
            rabbit = RabbitChannel(rabbit_url, service_name, pipelines)

            # Define the name of the queue shared by all instances of this worker.
            shared_queue = 'etl_service_' + service_name

            for method, properties, body in rabbit.consume(queue=shared_queue):
                data = json.loads(body)
                pipeline_name = data['pipeline']
                pipeline_cls = pipelines[pipeline_name]
                target_time = isodate.parse_datetime(data['target_time'])
                run_id = data['run_id']

                if method.exchange == mp.ANNOUNCE_PIPELINE_RUN_EXCHANGE:
                    pipeline = pipeline_cls(rabbit.conn, rabbit, service_name,
                                            pipeline_name, run_id)
                    # If it's a pipeline run announcement, then call get_targets
                    # and publish result.
                    targets = pipeline.get_targets(target_time)
                    logger.info("Acking pipeline run %s:%s:%s" %
                                (service_name, data['pipeline'], data['run_id']))
                    mp.ack_pipeline_run(rabbit, service_name, data['pipeline'],
                                        data['target_time'], run_id,
                                        targets)
                elif method.exchange == mp.ANNOUNCE_JOB_EXCHANGE:
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
                        succeeded = pipeline.make_target(target_time, target)

                        mp.end_job(rabbit, service_name, data['pipeline'],
                                   data['target_time'], data['target'],
                                   job_id, utc.now().isoformat(), succeeded)
                    else:
                        logging.info('Failed to claim job %s.' % job_id)
                rabbit.basic_ack(method.delivery_tag)

        except pika.exceptions.AMQPError as e:
            logger.exception('Unexpected RabbitMQ exception: %s.' % str(e))
            logger.info('Connection will be re-established in %s seconds!'
                        % SLEEP_INTERVAL_ON_RABBITMQ_EXCEPTION)
            time.sleep(SLEEP_INTERVAL_ON_RABBITMQ_EXCEPTION)
