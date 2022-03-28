"""The following attributes can be found in the presto query plan statistics of leaf nodes (e.g. TableScan, ScanFilterProject, etc.)"""
ROWS = 'rows'
ROW_SIZE = 'rowsSize'
CPU_COST = 'cpuCost'
MAX_MEMORY = 'maxMemory'
MAX_MEMORY_OUTPUT = 'maxMemoryWhenOutputting'
NETWORK_COST = 'networkCost'
