# A dummy service that implements the mettle protocol for one pipeline, called
# "bar".  The "bar" pipeline will make targets of "tmp/<target_time>/[0-9].txt".

import os
import json
import socket
from datetime import timedelta

import pika
import isodate
import utc

from mettle_protocol.settings import get_settings
import mettle_protocol as mp


class Pipeline(object):

    def __init__(self, rabbit, service_name, pipeline_name, job_id=None):
        self.log_line_num = 0
        self.rabbit = rabbit
        self.service_name = service_name
        self.pipeline_name = pipeline_name
        self.job_id = job_id

    def log(self, msg):
        if self.job_id is None:
            raise ValueError("Must set job_id to enable job logging.")
        mp.send_log_msg(self.rabbit, self.service_name, self.pipeline_name,
                        self.job_id, self.log_line_num, msg)
        self.log_line_num += 1

    def get_targets(self, target_time):
        raise NotImplementedError

    def get_expire_time(self, target_time, target, start_time):
        raise NotImplementedError

    def make_target(self, target_time, target):
        raise NotImplementedError

class FooPipeline(Pipeline):

    def get_targets(self, target_time):
        dirname = self._get_dir(target_time)
        targets = [str(x) for x in xrange(1, 11)]
        present = []
        absent = []
        for t in targets:
            if self._target_exists(target_time, t):
                present.append(t)
            else:
                absent.append(t)
        return {
            'present': present,
            'absent': absent,
        }

    def get_expire_time(self, target_time, target, start_time):
        """
        Given a target, and a UTC execution start time, return a UTC datetime
        for when the system should consider the job to have failed.
        """
        # Foo just hardcodes a 1 minute expiration time.
        return start_time + timedelta(minutes=1)

    def make_target(self, target_time, target):
        self.log("Making target %s." % target)
        try:
            if self._target_exists(target_time, target):
                self.log("%s already exists." % target)
            else:
                self.log("%s does not exist.  Creating." % target)
                filename = self._target_to_filename(target_time, target)
                dirname = os.path.dirname(filename)
                if not os.path.isdir(dirname):
                    os.makedirs(dirname)
                with open(filename, 'w') as f:
                    f.write('hello world!')
            return True
        except Exception as e:
            self.log("Error making target %s: %s" % (target, e))
            return False

    def _get_dir(self, target_time):
        return os.path.join('foo', target_time.isoformat())

    def _target_exists(self, target_time, target):
        filename = self._target_to_filename(target_time, target)
        if os.path.isfile(filename):
            return True

    def _target_to_filename(self, target_time, target):
        dirname = self._get_dir(target_time)
        return os.path.join(dirname, '%s.txt' % target)

def run_pipelines(service_name, rabbit_url, pipelines):
    # Expects 'pipelines' to be a dict of pipeline names and instances, where
    # the key for each entry is a tuple of (service_name, pipeline_name)
    hostname = socket.getfqdn()
    rabbit_conn = pika.BlockingConnection(pika.URLParameters(rabbit_url))
    rabbit = rabbit_conn.channel()
    mp.declare_exchanges(rabbit)

    queue_name = 'etl_service_' + service_name

    queue = rabbit.queue_declare(queue=queue_name, exclusive=False,
                                 durable=True)
    for name in pipelines:
        # For each pipeline we've been given, listen for both pipeline run
        # announcements and job announcements.
        routing_key = mp.build_routing_key(service_name, name)
        rabbit.queue_bind(exchange=mp.ANNOUNCE_PIPELINE_RUN_EXCHANGE,
                          queue=queue_name, routing_key=routing_key)
        rabbit.queue_bind(exchange=mp.ANNOUNCE_JOB_EXCHANGE,
                          queue=queue_name, routing_key=routing_key)

    for method, properties, body in rabbit.consume(queue=queue_name):
        data = json.loads(body)
        pipeline_name = data['pipeline']
        pipeline_cls = pipelines[pipeline_name]
        target_time = isodate.parse_datetime(data['target_time'])
        if method.exchange == mp.ANNOUNCE_PIPELINE_RUN_EXCHANGE:
            pipeline = pipeline_cls(rabbit, service_name, pipeline_name)
            # If it's a pipeline run announcement, then call get_targets and
            # publish result.
            targets = pipeline.get_targets(target_time)
            mp.ack_pipeline_run(rabbit, service_name, data['pipeline'],
                                data['target_time'], data['run_id'],
                                targets['present'], targets['absent'])
        elif method.exchange == mp.ANNOUNCE_JOB_EXCHANGE:
            job_id = data['job_id']
            pipeline = pipeline_cls(rabbit, service_name, pipeline_name, job_id)
            # If it's a job announcement, then publish ack, run job, then publish
            # completion.
            # publish ack
            now = utc.now()
            expire_time = pipeline.get_expire_time(target_time, data['target'], now)
            mp.ack_job(rabbit, service_name, data['pipeline'],
                       data['target_time'], data['target'], job_id, hostname,
                       now.isoformat(), expire_time.isoformat())

            # WOOO!  Actually do some work, here.
            succeeded = pipeline.make_target(target_time, data['target'])

            mp.end_job(rabbit, service_name, data['pipeline'],
                       data['target_time'], data['target'],
                       job_id, utc.now().isoformat(), succeeded)
        rabbit.basic_ack(method.delivery_tag)


def main():
    settings = get_settings()
    pipelines = {
        'bar': FooPipeline,
    }
    run_pipelines('foo', settings.rabbit_url, pipelines)

if __name__ == '__main__':
    main()
