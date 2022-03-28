"""The following operators can be found in presto"""
OUTPUT = 'Output'
AGGREGATE_FINAL = 'Aggregate(FINAL)'
AGGREGATE_PARTIAL = 'Aggregate(PARTIAL)'
LOCAL_EXCHANGE = 'LocalExchange'
REMOTE_EXCHANGE = 'RemoteStreamingExchange'
FILTER = 'Filter'
TABLE_SCAN = 'TableScan'
PROJECT = 'Project'
INNER_JOIN = 'InnerJoin'
CROSS_JOIN = 'CrossJoin'
# todo what about outer joins?

# types which will not be broken up
SCAN_FILTER_PROJECT = 'ScanFilterProject'
SCAN_FILTER = 'ScanFilter'
SCAN_PROJECT = 'ScanProject'

UNARY_OPERATORS = [OUTPUT, AGGREGATE_PARTIAL, AGGREGATE_FINAL, LOCAL_EXCHANGE, REMOTE_EXCHANGE, SCAN_FILTER_PROJECT, FILTER, PROJECT, TABLE_SCAN]
BINARY_OPERATORS = [INNER_JOIN, CROSS_JOIN]
LEAF_TYPES = [TABLE_SCAN]

ENCODED_TYPES = list(sorted(list(set(UNARY_OPERATORS + BINARY_OPERATORS) - {OUTPUT, SCAN_FILTER_PROJECT, SCAN_FILTER, SCAN_PROJECT})))
