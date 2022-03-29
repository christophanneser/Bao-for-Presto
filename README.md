# Bao Integration for Presto

## Requirements

- Initialize the git submodules (presto)
- Install python requirements using the file `requirements.txt`
- The benchmark driver will persist all data in a postgres database.
- Make the following environment variables available when running `driver.py`:
    - DB_HOST
    - DB_USER (must be owner of DB_SCHEMA to create the tables and functions)
    - DB_PASSWORD
    - DB_NAME
    - DB_SCHEMA

## Run the benchmarks

### Execution

The benchmark execution consists of two steps:

1. Approximate the query span (presto optimizers/rules that actually modify the query plan)
   ```
   driver.py --query_span --benchmark {job,stack,tpch} --catalog {presto catalog} --schema {presto schema}
   ```
2. Running a DP-based approach to find promising optimizer configurations and executing different plan alternatives.
   This will take some time.
   ```
   driver.py --record_time --dot --json --catalog {presto catalog} --schema {presto schema} --repeats 1
   ```
3. By now, the database should be filled with query spans and execution statistics for different plan alternatives.

### Docker

For convenience, we added a Dockerfile that sets up all components properly. TODO

## Code Formatting

All python files will be checked using `pylint` before they can be comitted. The code style is primarily based on
the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html), however, it allows much longer
lines (160 characters).
