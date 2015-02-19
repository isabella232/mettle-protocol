# A dummy service that implements the mettle protocol for one pipeline, called
# "bar".  The "bar" pipeline will make targets of "tmp/<target_time>/[0-9].txt".

import json
import logging
import random
import socket
import string
import uuid

import os
import pika
import isodate
import utc
import mettle_protocol.messages as mp


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Pipeline(object):
    assignment_wait_secs = 30

    def __init__(self, conn, chan, service_name, pipeline_name, job_id=None):
        self.log_line_num = 0
        self.conn = conn
        self.chan = chan
        self.service_name = service_name
        self.pipeline_name = pipeline_name
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
        if self.job_id is None:
            raise ValueError("Must set job_id to enable job logging.")
        mp.send_log_msg(self.chan, self.service_name, self.pipeline_name,
                        self.job_id, self.log_line_num, msg)
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
    # Expects 'pipelines' to be a dict of pipeline names and instances, where
    # the key for each entry is a tuple of (service_name, pipeline_name)
    rabbit_conn = pika.BlockingConnection(pika.URLParameters(rabbit_url))
    rabbit = rabbit_conn.channel()
    rabbit.basic_qos(prefetch_count=0)
    mp.declare_exchanges(rabbit)

    # Declare the queue shared by all instances of this worker.
    shared_queue = 'etl_service_' + service_name
    rabbit.queue_declare(queue=shared_queue, exclusive=False, durable=True)

    # als
    for name in pipelines:
        # For each pipeline we've been given, listen for both pipeline run
        # announcements and job announcements.
        routing_key = mp.pipeline_routing_key(service_name, name)
        rabbit.queue_bind(exchange=mp.ANNOUNCE_PIPELINE_RUN_EXCHANGE,
                          queue=shared_queue, routing_key=routing_key)
        rabbit.queue_bind(exchange=mp.ANNOUNCE_JOB_EXCHANGE,
                          queue=shared_queue, routing_key=routing_key)

    for method, properties, body in rabbit.consume(queue=shared_queue):
        data = json.loads(body)
        pipeline_name = data['pipeline']
        pipeline_cls = pipelines[pipeline_name]
        target_time = isodate.parse_datetime(data['target_time'])

        if method.exchange == mp.ANNOUNCE_PIPELINE_RUN_EXCHANGE:
            pipeline = pipeline_cls(rabbit_conn, rabbit, service_name,
                                    pipeline_name)
            # If it's a pipeline run announcement, then call get_targets and
            # publish result.
            targets = pipeline.get_targets(target_time)
            logger.info("Acking pipeline run %s:%s:%s" % (service_name,
                                                          data['pipeline'],
                                                          data['run_id']))
            mp.ack_pipeline_run(rabbit, service_name, data['pipeline'],
                                data['target_time'], data['run_id'],
                                targets)
        elif method.exchange == mp.ANNOUNCE_JOB_EXCHANGE:
            job_id = data['job_id']
            pipeline = pipeline_cls(rabbit_conn, rabbit, service_name,
                                    pipeline_name, job_id)
            # If it's a job announcement, then publish ack, run job,
            # then publish completion.
            # publish ack
            claimed = pipeline._claim_job(target_time, data['target'])

            if claimed:
                # WOOO!  Actually do some work here.
                succeeded = pipeline.make_target(target_time, data['target'])

                mp.end_job(rabbit, service_name, data['pipeline'],
                           data['target_time'], data['target'],
                           job_id, utc.now().isoformat(), succeeded)
            else:
                logging.info('Failed to claim job %s.' % job_id)
        rabbit.basic_ack(method.delivery_tag)
