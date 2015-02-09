import json

import pika

# This module provides functions and constants to implement the core protocol
# used by the timer, dispatcher, and ETL services.
ANNOUNCE_PIPELINE_RUN_EXCHANGE = 'mettle_announce_pipeline_run'
ACK_PIPELINE_RUN_EXCHANGE = 'mettle_ack_pipeline_run'
ANNOUNCE_JOB_EXCHANGE = 'mettle_announce_job'
ACK_JOB_EXCHANGE = 'mettle_ack_job'
END_JOB_EXCHANGE = 'mettle_end_job'
JOB_LOGS_EXCHANGE = 'mettle_job_logs'
PIKA_PERSISTENT_MODE = 2


def declare_exchanges(rabbit):
    for exchange in (ANNOUNCE_PIPELINE_RUN_EXCHANGE,
                     ACK_PIPELINE_RUN_EXCHANGE,
                     ANNOUNCE_JOB_EXCHANGE,
                     ACK_JOB_EXCHANGE,
                     END_JOB_EXCHANGE,
                     JOB_LOGS_EXCHANGE):
        rabbit.exchange_declare(exchange=exchange, type='topic', durable=True)


def build_routing_key(service_name, pipeline_name):
    return '.'.join([service_name, pipeline_name])


def announce_pipeline_run(rabbit, service_name, pipeline_name, target_time,
                          run_id):
    payload = {
        'service': service_name,
        'pipeline': pipeline_name,
        'run_id': run_id,
        'target_time': target_time,
    }

    print "Announcing pipeline run %s:%s:%s." % (service_name, pipeline_name,
                                                 target_time)
    rabbit.basic_publish(
        exchange=ANNOUNCE_PIPELINE_RUN_EXCHANGE,
        routing_key=build_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )


def ack_pipeline_run(rabbit, service_name, pipeline_name, target_time, run_id,
                     targets_present, targets_absent):
    print "Acking pipeline run %s:%s:%s." % (service_name, pipeline_name, run_id)

    payload = {
        'service': service_name,
        'pipeline': pipeline_name,
        'run_id': run_id,
        'target_time': target_time,
        'targets_present': targets_present,
        'targets_absent': targets_absent,
    }

    rabbit.basic_publish(
        exchange=ACK_PIPELINE_RUN_EXCHANGE,
        routing_key=build_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )


def announce_job(rabbit, service_name, pipeline_name, target_time, target, job_id):
    # 'target' should be a string that includes all the information that the ETL
    # service worker will need to produce this output.  If it's a particular
    # slice of rows in a DB table, for example, then 'target' should include the
    # LIMIT and OFFSET parameters.

    print "Announcing job %s:%s:%s:%s." % (service_name, pipeline_name, target,
                                        job_id)
    payload = {
        'service': service_name,
        'pipeline': pipeline_name,
        'target_time': target_time,
        'target': target,
        'job_id': job_id,
    }
    rabbit.exchange_declare(exchange=ANNOUNCE_JOB_EXCHANGE, type='topic',
                            durable=True)
    rabbit.basic_publish(
        exchange=ANNOUNCE_JOB_EXCHANGE,
        routing_key=build_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )


def ack_job(rabbit, service_name, pipeline_name, target_time, target, job_id,
            hostname, start_time, expires):
    print "Acking job %s:%s:%s:%s:%s." % (service_name, pipeline_name, target,
                                          job_id, hostname)
    payload = {
        'service': service_name,
        'pipeline': pipeline_name,
        'target_time': target_time,
        'target': target,
        'job_id': job_id,
        'hostname': hostname,
        'start_time': start_time,
        'expires': expires,
    }
    rabbit.basic_publish(
        exchange=ACK_JOB_EXCHANGE,
        routing_key=build_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )


def end_job(rabbit, service_name, pipeline_name, target_time, target, job_id,
            end_time, succeeded):
    print "Ending job %s:%s:%s:%s." % (service_name, pipeline_name, target,
                                       job_id)
    payload = {
        'service': service_name,
        'pipeline': pipeline_name,
        'target_time': target_time,
        'target': target,
        'job_id': job_id,
        'end_time': end_time,
        'succeeded': succeeded,
    }
    rabbit.basic_publish(
        exchange=END_JOB_EXCHANGE,
        routing_key=build_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )


def send_log_msg(rabbit, service_name, pipeline_name, job_id, msg):
    print "Job msg %s:%s:%s    %s" % (service_name, pipeline_name, job_id, msg)
    payload = {
        'service': service_name,
        'pipeline': pipeline_name,
        'job_id': job_id,
        'msg': msg,
    }
    rabbit.basic_publish(
        exchange=JOB_LOGS_EXCHANGE,
        routing_key=build_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )

