#! /bin/bash

# find required and effective rules without executing the plans
collect_rules() {
  BENCHMARK=$1
  CATALOG=$2
  SCHEMA=$3

  python3 driver.py --benchmark "${BENCHMARK}" --catalog "${CATALOG}" --schema "${SCHEMA}" --query_span
}

# execute the different plans and create optimizer configs using dynamic programming approach
run_dynamic_programming() {
  BENCHMARK=$1
  CATALOG=$2
  SCHEMA=$3

  python3 driver.py --benchmark "${BENCHMARK}" --catalog "${CATALOG}" --schema "${SCHEMA}" --record_time --repeats 7 --dot --json
}

find_optimizer_configs() {
  BENCHMARK=$1
  CATALOG=$2
  SCHEMA=$3

  collect_rules "${BENCHMARK}" "${CATALOG}" "${SCHEMA}"
  run_dynamic_programming "${BENCHMARK}" "${CATALOG}" "${SCHEMA}"
}

find_optimizer_configs stack hive stackoverflow
