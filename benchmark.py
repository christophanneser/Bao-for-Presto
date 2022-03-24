"""This module coordinates the query span approximation and the generation of new optimizer configurations for a query"""
import asyncio
import operator
import cluster_controller
import prestodb
import storage
import settings
import hashlib
from optimizer_config import OptimizerConfig
from functools import reduce
from presto_connector import get_session
from custom_logging import logging
from session_properties import BAO_DISABLED_OPTIMIZERS, BAO_DISABLED_RULES, BAO_ENABLE, BAO_EXECUTE_QUERY, BAO_EXPORT_GRAPHVIZ, BAO_EXPORT_JSON, \
    BAO_EXPORT_TIMES, BAO_GET_QUERY_SPAN

FLIGHTS_QUERIES_PATH = 'queries/flights/'
TAXI_QUERIES_PATH = 'queries/taxi/'
JOB_QUERIES_PATH = 'queries/job/'
TPCH_QUERIES_PATH = 'queries/tpch/'
TPCDS_QUERIES_PATH = 'queries/tpcds/'
STACK_QUERIES_PATH = 'queries/stackoverflow/'


async def _receive_query_plans_async():
    callback_server = get_session().callback_server
    if settings.EXPORT_GRAPHVIZ:
        logging.info('receive dot plans')
        callback_server.handle_request()
        callback_server.handle_request()
        logging.info('received both dot plans')
    if settings.EXPORT_JSON:
        logging.info('receive json plans')
        callback_server.handle_request()
        callback_server.handle_request()
        logging.info('received both json plans')

    status = get_session().status
    settings.LOGICAL_DOT = status.logical_dot
    settings.FRAGMENTED_DOT = status.fragmented_dot
    settings.LOGICAL_JSON = status.logical_json
    settings.FRAGMENTED_JSON = status.fragmented_json


def load_query(filepath):
    with open(filepath, 'r') as f:
        lines = f.readlines()
        lines = filter(lambda s: not s.startswith('--') and not s == '', lines)
        return ''.join(lines).replace('\\s', ' ').replace(';', '')


async def execute(cur, query_string):
    cur = cur.cursor()
    cur.execute(query_string)
    try:
        result = cur.fetchall()
        return result, cur
    except prestodb.exceptions.PrestoQueryError as e:
        logging.error('Received an error during query execution: %s', str(e))
        return e


async def _run_query_async(conn, query_string):
    return await execute(conn, query_string)


async def execute_async(conn, query_string):
    query_task = _run_query_async(conn, query_string)
    query_plans_task = _receive_query_plans_async()
    responses = await asyncio.gather(query_task, query_plans_task)
    return responses


def exec_query(conn, query_string, set_config_variable=False):
    if set_config_variable or not settings.EXPORT_GRAPHVIZ:
        cur = conn.cursor()
        cur.execute(query_string)
        result = cur.fetchall()
        return result, cur
    else:
        # asynchronously process callbacks from presto containing the dot representations
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(execute_async(conn, query_string))[0]
        if isinstance(result, prestodb.exceptions.PrestoQueryError):
            raise result
        return result  # result = (result, cursor containing query stats)


def reset_presto_config(conn):
    """Reset the presto session """
    configs = [BAO_EXPORT_GRAPHVIZ, BAO_EXPORT_JSON, BAO_EXPORT_TIMES, BAO_ENABLE, BAO_GET_QUERY_SPAN]
    for config in configs:
        set_presto_config(conn, config, False)
    set_presto_config(conn, BAO_EXECUTE_QUERY, True)
    enable_all_optimizers_and_rules(conn)


def set_presto_config(conn, setting, enable=True):
    try:
        settings.PRESTO_SETTINGS[setting] = enable
        exec_query(conn, 'SET session {0} = {1}'.format(setting, 'true' if enable else 'false'), set_config_variable=True)
    except prestodb.exceptions.PrestoUserError:
        pass


def enable_all_optimizers_and_rules(conn):
    exec_query(conn, f'SET session {BAO_DISABLED_OPTIMIZERS} = \'\'', set_config_variable=True)
    exec_query(conn, f'SET session {BAO_DISABLED_RULES} = \'\'', set_config_variable=True)


def set_optimizer_config(conn, setting):
    try:
        exec_query(conn, setting, set_config_variable=True)
    except prestodb.exceptions.PrestoUserError:
        pass


def run(conn, query_path):
    query_string = load_query(query_path)
    try:
        return exec_query(conn, query_string)
    except prestodb.exceptions.PrestoUserError as e:
        logging.error('Error with query: %s: %s', query_path, str(e))
        return e
    except prestodb.exceptions.PrestoQueryError as e:
        logging.error('Error with query: %s: %s', query_path, str(e))
        return e


def register_query_config_and_measurement(query_path, disabled_rules, cursor=None, initial_call=False, result=None):
    get_session().callback_server.handle_request()  # wait for execution stats
    plan_hash = get_session().status.execution_stats['plan_hash']
    logical_dot = settings.LOGICAL_DOT if settings.EXPORT_GRAPHVIZ else None
    fragmented_dot = settings.FRAGMENTED_DOT if settings.EXPORT_GRAPHVIZ else None
    logical_json = settings.LOGICAL_JSON if settings.EXPORT_JSON else None
    fragmented_json = settings.FRAGMENTED_JSON if settings.EXPORT_JSON else None
    is_duplicate = storage.register_query_config(query_path, disabled_rules, logical_dot, fragmented_dot, logical_json, fragmented_json, plan_hash)
    is_duplicate = False  # fixme
    if is_duplicate:
        logging.info('Plan hash already known')
    if not (is_duplicate or initial_call):
        register_time_measurement(query_path, disabled_rules, cursor, result)

    settings.LOGICAL_DOT = None
    settings.FRAGMENTED_DOT = None
    return is_duplicate


def register_time_measurement(query_path, disabled_rules, cursor, result):
    assert cursor is not None
    assert result is not None

    def hash_sql_result():
        """Generate a hash fingerprint for the result retrieved from presto to assert that results are (probably) identical.
        Its important to round floats here, e.g. 2 decimal places."""
        flattened_result = reduce(operator.concat, result)
        normalized_result = tuple(map(lambda item: round(item, 2) if isinstance(item, float) else item, flattened_result))
        md5 = hashlib.md5()
        for item in normalized_result:
            md5.update(str(item).encode())
        return md5.digest()  # '{:02x}'.format(md5.digest())

    # check if results match
    result_fingerprint = hash_sql_result()
    if not storage.register_query_fingerprint(query_path, result_fingerprint):
        logging.warning('Result fingerprint=%s does not match existing fingerprints!', result_fingerprint)

    status = get_session().status
    if not status.execution_stats['query_id'] == cursor.stats['queryId']:
        logging.fatal('WRONG EXECUTION STATS RECEIVED: %s vs %s', status.execution_stats['query_id'], cursor.stats['queryId'])
        status.execution_stats = None
        return
    # wait for presto server to call back
    execution_stats = status.execution_stats
    assert execution_stats is not None
    storage.register_measurement(
        query_path,
        disabled_rules,
        elapsed=execution_stats['elapsed'],
        planning=execution_stats['planning'],
        scheduling=execution_stats['scheduling'],
        running=execution_stats['running'],
        finishing=execution_stats['finishing'],
        cpu=execution_stats['cpu'],
        input_data_size=execution_stats['input_data_size'],
        nodes=cursor.stats['nodes'])
    get_session().status.execution_stats = None


def run_query_with_optimizer_configs(connection, query_path):
    """Use dynamic programming to find good optimizer configs"""
    logging.info('Start DP for query %s', query_path)

    set_presto_config(connection, BAO_EXPORT_TIMES, True)

    config = OptimizerConfig(query_path)
    num_duplicates = 0
    while config.has_next():
        enable_all_optimizers_and_rules(connection)
        optimizer_configs = config.next()

        for setting in optimizer_configs:
            set_optimizer_config(connection, setting)

        # skip configs which have been executed previously
        if storage.check_for_existing_measurements(
            query_path, config.get_disabled_opts_rules()):
            pass  # continue

        # check first if this config generates a new query plan (prevent actual execution of plan)
        set_presto_config(connection, 'execute_query', False)
        if cluster_controller.restart_if_required():
            for setting in optimizer_configs:
                set_optimizer_config(connection, setting)
        run(connection, query_path)
        set_presto_config(connection, 'execute_query', True)

        if register_query_config_and_measurement(query_path, config.get_disabled_opts_rules(), initial_call=True):
            num_duplicates += 1
            continue

        run_config(config, connection, query_path, optimizer_configs)

    enable_all_optimizers_and_rules(connection)
    run_config(config, connection, query_path)  # re-run default config with all optimizers being enabled again

    logging.info('Found %s duplicated query plans!', num_duplicates)


def run_config(config, conn, query_path, optimizer_configs=frozenset()):
    for repeat in range(settings.REPEATS):
        # restart server if required
        if cluster_controller.restart_if_required():
            for setting in optimizer_configs:
                set_optimizer_config(conn, setting)

        result = run(conn, query_path)
        if isinstance(result, prestodb.exceptions.PrestoQueryError):
            # configuration does not work -> disabled optimizers/rules are required
            if result.error_name == 'NO_NODES_AVAILABLE':
                # server is not in good shape right now -> re-run experiment please
                cluster_controller.NO_NODES_AVAILABLE_ERROR = True
                repeat -= 1
                continue
            else:
                logging.fatal('optimizer %s cannot be disabled for %s - SKIP this config', config.get_disabled_opts_rules(), query_path)
                break

        if register_query_config_and_measurement(query_path, config.get_disabled_opts_rules(), result=result[0], cursor=result[1]):
            # config results in already known query plan!
            break


def run_query_get_span(connection, query_path):
    """Given the presto connection and the path to a sql file, get the query span and store it in the database"""

    set_presto_config(connection, BAO_GET_QUERY_SPAN, True)

    async def _receive_span_async():
        # query span comprises 4 components (required/effective rules/optimizers);
        for _ in range(4):
            logging.debug('wait for query span callback ...')
            get_session().callback_server.handle_request()
            logging.debug('callback received!')

    def _get_span_async():
        return asyncio.gather(_run_query_async(connection, query_string), _receive_span_async())

    cluster_controller.restart_if_required()
    logging.info('Approximate query span for query: %s', query_path)
    storage.register_query(query_path)
    query_string = load_query(query_path)

    # asynchronously fetch query spans from presto server
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_get_span_async())
    query_span = get_session().status.query_span

    assert query_span.effective_optimizers is not None
    for rule in query_span.effective_optimizers:
        storage.register_optimizer(query_path, rule, 'query_effective_optimizers')

    assert query_span.required_optimizers is not None
    for rule in query_span.required_optimizers:
        storage.register_optimizer(query_path, rule, 'query_required_optimizers')

    assert query_span.effective_rules is not None
    for rule in query_span.effective_rules:
        storage.register_rule(query_path, rule, 'query_effective_rules')

    assert query_span.required_rules is not None
    for rule in query_span.required_rules:
        storage.register_rule(query_path, rule, 'query_required_rules')
