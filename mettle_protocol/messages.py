import json
import logging

import pika


# This module provides functions and constants to implement the core protocol
# used by the timer, dispatcher, and ETL services.
ANNOUNCE_SERVICE_EXCHANGE = 'mettle_announce_service'
ANNOUNCE_PIPELINE_RUN_EXCHANGE = 'mettle_announce_pipeline_run'
ACK_PIPELINE_RUN_EXCHANGE = 'mettle_ack_pipeline_run'
NACK_PIPELINE_RUN_EXCHANGE = 'mettle_nack_pipeline_run'
CLAIM_JOB_EXCHANGE = 'mettle_claim_job'
END_JOB_EXCHANGE = 'mettle_end_job'
JOB_LOGS_EXCHANGE = 'mettle_job_logs'
PIKA_PERSISTENT_MODE = 2

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def declare_exchanges(rabbit):
    for exchange in (ANNOUNCE_SERVICE_EXCHANGE,
                     ANNOUNCE_PIPELINE_RUN_EXCHANGE,
                     ACK_PIPELINE_RUN_EXCHANGE,
                     NACK_PIPELINE_RUN_EXCHANGE,
                     CLAIM_JOB_EXCHANGE,
                     END_JOB_EXCHANGE,
                     JOB_LOGS_EXCHANGE):
        rabbit.exchange_declare(exchange=exchange, type='topic', durable=True)


def pipeline_routing_key(service_name, pipeline_name):
    return '.'.join([service_name, pipeline_name])


def service_queue_name(service_name):
    return 'etl_service_' + service_name


def mq_escape(chars):
    """
    Given a string that you might want to use in a RabbitMQ routing key, replace
    any dots, stars, or hashes with underscores, so it won't throw off Rabbit's
    bindings.
    """
    return chars.replace('*', '_').replace('.', '_').replace('#', '_')


def announce_service(rabbit, service_name, pipeline_names):
    payload = {
        'service': service_name,
        'pipeline_names': pipeline_names,
    }
    logger.info("Announcing service %s:%s." % (service_name,
                                               ', '.join(pipeline_names)))

    rabbit.basic_publish(
        exchange=ANNOUNCE_SERVICE_EXCHANGE,
        routing_key=service_name,
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )



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


def find_cycle(targets):
    """
    Given a dict representing a target dependency graph, return a list of any
    nodes involved in a dependency cycle.  Returns the first cycle found.
    """
    # Thank you Guido
    # http://neopythonic.blogspot.com/2009/01/detecting-cycles-in-directed
    # -graph.html
    todo = set(targets.keys())
    while todo:
        node = todo.pop()
        stack = [node]
        while stack:
            top = stack[-1]
            for node in targets[top]:
                if node in stack:
                    return stack[stack.index(node):]
                if node in todo:
                    stack.append(node)
                    todo.remove(node)
                    break
            else:
                node = stack.pop()
    return None


def validate_targets_graph(targets):
    for k, v in targets.items():
        # all keys are strings
        assert isinstance(k, basestring), "%s is not a string" % k
        # all values are lists
        assert isinstance(v, list), "%s is not a list" % v
        # each item in each value list is also a key in the dict
        for dep in v:
            assert dep in targets, "%s is not a target" % dep

    # No cycles
    cycle_nodes = find_cycle(targets)
    if cycle_nodes:
        raise AssertionError(
            "Found cycle in target graph involving these targets:"
            ", ".join(cycle_nodes))


def ack_pipeline_run(rabbit, service_name, pipeline_name, target_time, run_id,
                     targets, target_parameters):
    # targets should be a dictionary like this:
    # targets = {
    # "file1.txt": [],
    #   "file2.txt": [],
    #   "file3.txt": [],
    #   "manifest.txt": ["file1.txt", "file2.txt", "file3.txt"]
    # }
    # Where the key in each dict is a string representing the target to be made,
    # and each value is a list of that target's dependencies.
    #
    # Each depependency must itself be a target (key) in the dict, and cyclical
    # dependencies are not allowed.

    logger.info("Acking pipeline run %s:%s:%s." % (service_name, pipeline_name,
                                                   run_id))

    validate_targets_graph(targets)

    payload = {
        'service': service_name,
        'pipeline': pipeline_name,
        'run_id': run_id,
        'target_time': target_time,
        'targets': targets,
        'target_parameters': target_parameters,
    }

    rabbit.basic_publish(
        exchange=ACK_PIPELINE_RUN_EXCHANGE,
        routing_key=pipeline_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )


def nack_pipeline_run(rabbit, service_name, pipeline_name, run_id,
                      reannounce_time, message):
    # A service telling Mettle that it can't run a particular pipeline right
    # now.  'retry' should be a boolean saying whether Mettle should bother
    # re-announcing this run later.
    logger.info("Nacking pipeline run %s:%s:%s." % (service_name, pipeline_name,
                                                    run_id))

    payload = {
        'service': service_name,
        'pipeline': pipeline_name,
        'run_id': run_id,
        'reannounce_time': reannounce_time,
        'message': message,
    }

    rabbit.basic_publish(
        exchange=NACK_PIPELINE_RUN_EXCHANGE,
        routing_key=pipeline_routing_key(service_name, pipeline_name),
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )


def queue_job(rabbit, queue_name, service_name, pipeline_name, target_time, target,
                 target_parameters, run_id, job_id):
    # 'target' should be a string that includes all the information that the ETL
    # service worker will need to produce this output.  If it's a particular
    # slice of rows in a DB table, for example, then 'target' should include the
    # LIMIT and OFFSET parameters.

    logger.info("Announcing job %s:%s:%s:%s:%s." % (service_name, pipeline_name,
                                                    run_id, target, job_id))
    payload = {
        'service': service_name,
        'pipeline': pipeline_name,
        'target_time': target_time,
        'target': target,
        'target_parameters': target_parameters,
        'run_id': run_id,
        'job_id': job_id,
    }
    rabbit.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            delivery_mode=PIKA_PERSISTENT_MODE,
        ),
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
                                        correlation_id=corr_id, )
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


def send_log_msg(rabbit, service_name, pipeline_name, run_id, target, job_id,
                 line_num, msg):
    logger.info("Job msg %s:%s:%s:%s:%s" % (service_name, pipeline_name, job_id,
                                            run_id, msg))
    routing_key = '.'.join([
        service_name,
        pipeline_name,
        str(run_id),
        mq_escape(target),
        str(job_id),
    ])
    payload = {
        'service': service_name,
        'pipeline': pipeline_name,
        'run_id': run_id,
        'job_id': job_id,
        'line_num': line_num,
        'msg': msg,
    }
    rabbit.basic_publish(
        exchange=JOB_LOGS_EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=PIKA_PERSISTENT_MODE)
    )

