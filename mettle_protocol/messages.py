import json
import logging

import pika

# This module provides functions and constants to implement the core protocol
# used by the timer, dispatcher, and ETL services.
ANNOUNCE_PIPELINE_RUN_EXCHANGE = 'mettle_announce_pipeline_run'
ACK_PIPELINE_RUN_EXCHANGE = 'mettle_ack_pipeline_run'
ANNOUNCE_JOB_EXCHANGE = 'mettle_announce_job'
CLAIM_JOB_EXCHANGE = 'mettle_claim_job'
END_JOB_EXCHANGE = 'mettle_end_job'
JOB_LOGS_EXCHANGE = 'mettle_job_logs'
PIKA_PERSISTENT_MODE = 2

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def declare_exchanges(rabbit):
    for exchange in (ANNOUNCE_PIPELINE_RUN_EXCHANGE,
                     ACK_PIPELINE_RUN_EXCHANGE,
                     ANNOUNCE_JOB_EXCHANGE,
                     CLAIM_JOB_EXCHANGE,
                     END_JOB_EXCHANGE,
                     JOB_LOGS_EXCHANGE):
        rabbit.exchange_declare(exchange=exchange, type='topic', durable=True)


def pipeline_routing_key(service_name, pipeline_name):
    return '.'.join([service_name, pipeline_name])


def announce_pipeline_run(rabbit, service_name, pipeline_name, target_time,
                          run_id):
    payload = {
        'service': service_name,
        'pipeline': pipeline_name,
        'run_id': run_id,
        'target_time': target_time,
    }

    logger.info("Announcing pipeline run %s:%s:%s." % (service_name,
                                                       pipeline_name,
                                                       target_time))
    rabbit.basic_publish(
        exchange=ANNOUNCE_PIPELINE_RUN_EXCHANGE,
        routing_key=pipeline_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )


def ack_pipeline_run(rabbit, service_name, pipeline_name, target_time, run_id,
                     targets_present, targets_absent):
    logger.info("Acking pipeline run %s:%s:%s." % (service_name, pipeline_name,
                                                   run_id))

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
        routing_key=pipeline_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )


def announce_job(rabbit, service_name, pipeline_name, target_time, target, job_id):
    # 'target' should be a string that includes all the information that the ETL
    # service worker will need to produce this output.  If it's a particular
    # slice of rows in a DB table, for example, then 'target' should include the
    # LIMIT and OFFSET parameters.

    logger.info("Announcing job %s:%s:%s:%s." % (service_name, pipeline_name,
                                                 target, job_id))
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
        routing_key=pipeline_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )


def claim_job(rabbit, job_id, worker_name, start_time, expires, corr_id):
    logger.info("Claiming job %s:%s:%s" % (job_id, worker_name, corr_id))
    payload = {
        'job_id': job_id,
        'worker_name': worker_name,
        'start_time': start_time,
        'expires': expires,
    }
    rabbit.basic_publish(
        exchange=CLAIM_JOB_EXCHANGE,
        routing_key=worker_name,
        body=json.dumps(payload),
        properties=pika.BasicProperties(reply_to=worker_name,
                                        correlation_id=corr_id,)
    )


def grant_job(rabbit, worker_name, corr_id, granted):
    # This method is not like the others.  While those messages publish to topic
    # exchanges, bound to shared queues, and have JSON payloads, this message
    # publishes to the special "default" exchange, directly to a worker-specific
    # queue, and has a payload of '0' or '1', letting a specific worker know
    # whether its job claim has been granted or not.

    # In other words, while the other messages are broadcast and queued, this
    # message is sent directly as an RPC response.
    rabbit.basic_publish(
        exchange='',
        routing_key=worker_name,
        properties=pika.BasicProperties(correlation_id=corr_id),
        body='1' if granted else '0',
    )


def end_job(rabbit, service_name, pipeline_name, target_time, target, job_id,
            end_time, succeeded):
    logger.info("Ending job %s:%s:%s:%s." % (service_name, pipeline_name,
                                             target, job_id))
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
        routing_key=pipeline_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )


def send_log_msg(rabbit, service_name, pipeline_name, job_id, line_num, msg):
    logger.info("Job msg %s:%s:%s    %s" % (service_name, pipeline_name, job_id,
                                            msg))
    payload = {
        'service': service_name,
        'pipeline': pipeline_name,
        'job_id': job_id,
        'line_num': line_num,
        'msg': msg,
    }
    rabbit.basic_publish(
        exchange=JOB_LOGS_EXCHANGE,
        routing_key=pipeline_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )

