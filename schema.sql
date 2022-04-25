-- create function to easily calculate medians
CREATE OR REPLACE FUNCTION _final_median(numeric[])
    RETURNS numeric AS
$$
SELECT AVG(val)
FROM (
         SELECT val
         FROM unnest($1) val
         ORDER BY 1
         LIMIT 2 - MOD(array_upper($1, 1), 2) OFFSET CEIL(array_upper($1, 1) / 2.0) - 1
     ) sub
$$
    LANGUAGE 'sql' IMMUTABLE;
--------------------------------------------------------------------------------
CREATE
OR
REPLACE
AGGREGATE median(numeric) (
    sfunc = array_append,
    STYPE =numeric[],
    finalfunc =_final_median,
    initcond = '{}'
    );
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS queries
(
    id                 SERIAL PRIMARY KEY,
    query_path         varchar(256) UNIQUE,
    result_fingerprint BYTEA
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_required_optimizers
(
    query_id     INTEGER REFERENCES queries,
    optimizer_id TEXT,
    PRIMARY KEY (query_id, optimizer_id)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_effective_optimizers
(
    query_id     INTEGER REFERENCES queries,
    optimizer_id TEXT,
    PRIMARY KEY (query_id, optimizer_id)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_required_rules
(
    query_id INTEGER REFERENCES queries,
    rule     TEXT,
    PRIMARY KEY (query_id, rule)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_effective_rules
(
    query_id INTEGER REFERENCES queries,
    rule     TEXT,
    PRIMARY KEY (query_id, rule)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS query_optimizer_configs
(
    id                   SERIAL PRIMARY KEY,
    query_id             INTEGER REFERENCES queries,
    disabled_rules       TEXT,
    unoptimized_plan_dot TEXT,
    logical_plan_dot     TEXT,
    fragmented_plan_dot  TEXT,
    logical_plan_json    TEXT,
    fragmented_plan_json TEXT,
    num_disabled_rules   INTEGER,
    hash                 INTEGER, -- the hash value of the optimizer query plan
    duplicated_plan      BOOLEAN DEFAULT FALSE,
    UNIQUE (query_id, disabled_rules)
);
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS measurements
(
    query_optimizer_config_id INTEGER REFERENCES query_optimizer_configs,
    elapsed                   INTEGER,
    planning                  INTEGER,
    scheduling                INTEGER,
    running                   INTEGER,
    finishing                 INTEGER,
    machine                   TEXT,
    time                      TIMESTAMP,
    cpu_time                  DECIMAL,
    input_data_size           BIGSERIAL,
    nodes                     INTEGER
);
--------------------------------------------------------------------------------
