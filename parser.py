"""Parser for the arguments passed to the benchmark driver"""
import argparse


def get_parser():
    parser = argparse.ArgumentParser(description='CLI for PrestoDB with BAO-Query-Optimizer')
    parser.add_argument('--query_span', help='get the query span per rule', action='store_true')
    parser.add_argument('--record_time', help='run each of the query with different optimizer configs and record the times', action='store_true')

    parser.add_argument('--benchmark', help='which benchmark should be executed [tpch, tpcds, job, flights, taxi]', type=str, default='tpch')
    parser.add_argument('--explain', help='explain the query', action='store_true')
    parser.add_argument('--number', help='execute tpch query by number', type=int)
    parser.add_argument('--dot', help='create graphviz logical and fragmented plan', action='store_true')
    parser.add_argument('--json', help='create json plans', action='store_true')

    parser.add_argument('--catalog', help='presto catalog to query', type=str, default='tpch')
    parser.add_argument('--schema', help='schema to query', type=str, default='tiny')
    parser.add_argument('--repeats', help='repeat queries', type=int, default=1)
    parser.add_argument('--drop_caches', help='drop fs caches before each run (requires root)', action='store_true')
    return parser
