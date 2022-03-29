""" Documentation: """
import os
import glob
import signal
import sys

from parser import get_parser
import benchmark
from presto_connector import get_session
from benchmark import TPCH_QUERIES_PATH, JOB_QUERIES_PATH, \
    STACK_QUERIES_PATH, set_presto_config, reset_presto_config, run_query_get_span, run_query_with_optimizer_configs
import settings
from custom_logging import bao_logging
from session_properties import BAO_EXPORT_GRAPHVIZ, BAO_EXPORT_JSON


def signal_handler(sig, frame):
    """Reset the current presto session in case of unexpected errors"""
    reset_presto_config(get_session().connection)
    get_session().callback_server.server_close()
    print(f'Stop driver as it received signal={sig} (frame={frame})! You pressed Ctrl+C! Reset presto configs!')
    sys.exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)

    presto_session = get_session()
    presto_session.restart_callback_server()

    parser = get_parser()
    args = parser.parse_args()
    settings.REPEATS = args.repeats
    settings.EXPORT_GRAPHVIZ = args.dot
    settings.EXPORT_JSON = args.json

    reset_presto_config(presto_session.get_connection())
    set_presto_config(presto_session.get_connection(), BAO_EXPORT_GRAPHVIZ, args.dot)
    set_presto_config(presto_session.get_connection(), BAO_EXPORT_JSON, args.json)

    assert args.query_span or args.record_time
    benchmark.ENABLE_DROP_CACHES = args.drop_caches

    RUN_QUERY = None
    if args.query_span:
        RUN_QUERY = run_query_get_span
        bao_logging.info('approximate query spans')
    elif args.record_time:
        RUN_QUERY = run_query_with_optimizer_configs
        bao_logging.info('collect measurements')

    assert RUN_QUERY is not None
    print('connect to catalog:schema={0}:{1}'.format(args.catalog,
                                                     args.schema))

    presto_session.set_catalog(args.catalog)
    presto_session.set_schema(args.schema)

    if args.benchmark == 'job':
        queries = sorted(
            list(
                filter(lambda q: q.endswith('.sql') and q != 'schema_job.sql',
                       os.listdir(JOB_QUERIES_PATH))))
        for query in queries:
            print('run JOB Q{0}...'.format(query))
            RUN_QUERY(presto_session.get_connection(), '{0}{1}'.format(JOB_QUERIES_PATH, query))
    elif args.benchmark == 'stack':
        queries = glob.iglob(STACK_QUERIES_PATH + '**/*.sql', recursive=True)
        for query in list(queries)[400:600]:
            if query.endswith('schema.sql'):
                continue
            print(query)
            RUN_QUERY(presto_session.get_connection(), query)
    elif args.benchmark == 'tpch':
        for query in range(1, 23):
            if query in [2]:
                continue
            print('run TPC-H Q{0}...'.format(query))
            PATH = '{0}{1}.sql'.format(TPCH_QUERIES_PATH, query)
            RUN_QUERY(presto_session.get_connection(), PATH)
    elif args.benchmark == 'example':
        PATH = './presentation/query.sql'
        RUN_QUERY(presto_session.get_connection(), PATH)

    reset_presto_config(presto_session.get_connection())
    presto_session.callback_server.server_close()
