"""This module implements the connection to the database."""
import json
from custom_logging import bao_logging
import os
import numpy as np
import pandas as pd
import random
import socket
import sqlalchemy
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.exc import IntegrityError

SCHEMA_FILE = 'schema.sql'
ENGINE = None


def _db():
    global ENGINE
    if ENGINE is None:
        user = os.getenv('DB_USER')
        database = os.getenv('DB_NAME')
        password = os.getenv('DB_PASSWORD')

        host = os.getenv('DB_HOST')
        if host is None:
            host = os.getenv('POSTGRES_SERVICE_HOST')
        assert host is not None

        url = f'postgresql://{user}:{password}@{host}:5432/{database}'
        bao_logging.info('Connect to database: %s', url)
        ENGINE = create_engine(url)

    schema = os.getenv('DB_SCHEMA')
    conn = ENGINE.connect()
    conn.execute(f'SET search_path TO {schema}')

    with open(SCHEMA_FILE, encoding='utf-8') as f:
        schema = f.read()
    schema = schema.split('\n')
    schema = '\n'.join(filter(lambda line: not line.startswith('--'), schema))

    for statement in schema.split(';'):
        if len(statement.strip()) > 0:
            conn.execute(statement)
    return conn


def register_query(query_path):
    with _db() as conn:
        try:
            stmt = text('INSERT INTO queries (query_path, result_fingerprint) VALUES ( :query_path, :result_fingerprint )')
            conn.execute(stmt, query_path=query_path, result_fingerprint=None)
        except IntegrityError:
            pass


def register_query_fingerprint(query_path, fingerprint):
    with _db() as conn:
        result = conn.execute(text('SELECT result_fingerprint FROM queries WHERE query_path= :query_path'), query_path=query_path).fetchone()[0]
        if result is None:
            conn.execute(
                text('UPDATE queries SET result_fingerprint = :fingerprint WHERE query_path = :query_path;'), fingerprint=fingerprint, query_path=query_path)
            return True
        elif bytes(result) != fingerprint:
            return False  # fingerprints do not match
        return True


def register_optimizer(query_path, optimizer, table):
    with _db() as conn:
        try:
            stmt = text(f'INSERT INTO {table} (query_id, optimizer_id) '
                        'SELECT id, :optimizer FROM queries WHERE query_path = :query_path')
            conn.execute(stmt, optimizer=optimizer, query_path=query_path)
        except IntegrityError:
            pass  # do not store duplicates


def best_alternative_configuration(benchmark=None):
    class OptimizerConfigResult:
        def __init__(self, path, num_disabled_rules, runtime, runtime_baseline, savings, disabled_rules, rank):
            self.path = path
            self.num_disabled_rules = num_disabled_rules
            self.runtime = runtime
            self.runtime_baseline = runtime_baseline
            self.savings = savings
            self.disabled_rules = disabled_rules
            self.rank = rank

    stmt = """
       with default_plans (query_path, running_time) as (
        select q.query_path, median(m.running + m.finishing)
        from queries q,
             query_optimizer_configs qoc,
             measurements m
        where q.id = qoc.query_id
          and qoc.id = m.query_optimizer_config_id
          and qoc.num_disabled_rules = 0
          and qoc.disabled_rules = 'None'
        group by q.query_path, qoc.num_disabled_rules, qoc.disabled_rules),
         results(query_path, num_disabled_rules, runtime, runtime_baseline, savings, disabled_rules, rank) as (
             select q.query_path,
                    qoc.num_disabled_rules,
                    median(m.running + m.finishing),
                    dp.running_time,
                    (dp.running_time - median(m.running + m.finishing)) / dp.running_time as savings,
                    qoc.disabled_rules,
                    dense_rank() over (
                        partition by q.query_path
                        order by (dp.running_time - median(m.running + m.finishing)) / dp.running_time desc ) as ranki
             from queries q,
                  query_optimizer_configs qoc,
                  measurements m,
                  default_plans dp
             where q.id = qoc.query_id
               and qoc.id = m.query_optimizer_config_id
               and dp.query_path = q.query_path
               and qoc.num_disabled_rules > 0
             group by q.query_path, qoc.num_disabled_rules, qoc.disabled_rules, dp.running_time
             order by savings desc)
    select *
    from results
    where rank = 1
    and query_path like '%{0}%'
    order by savings desc; 
    """
    stmt = stmt.format('' if benchmark is None else benchmark)

    _db()
    cursor = ENGINE.raw_connection().cursor()
    schema = os.getenv('DB_SCHEMA')
    cursor.execute(f'SET search_path TO {schema}')
    cursor.execute(stmt)
    return [OptimizerConfigResult(*row) for row in cursor.fetchall()]


class Measurement:
    """This class stores the measurement for a certain query and optimizer configuration"""

    def __init__(self, query_path, query_id, optimizer_config, disabled_rules,
                 num_disabled_rules, plan_json, running_time, cpu_time):
        self.query_path = query_path
        self.query_id = query_id
        self.optimizer_config = optimizer_config
        self.disabled_rules = disabled_rules
        self.num_disabled_rules = num_disabled_rules
        self.plan_json = plan_json
        self.running_time = running_time
        self.cpu_time = cpu_time


def experience(benchmark=None, training_ratio=0.8):
    """Get experience to train BAO"""
    stmt = """select qu.query_path, q.query_id, q.id,  q.disabled_rules, q.num_disabled_rules, q.logical_plan_json, median(running+finishing), median(cpu_time)
            from measurements m, query_optimizer_configs q, queries qu
            where m.query_optimizer_config_id = q.id
              and q.logical_plan_json != 'None' 
              and qu.id = q.query_id
              and qu.query_path ILIKE '%{0}%'
            group by qu.query_path, q.query_id, q.id, q.logical_plan_json, q.disabled_rules, q.num_disabled_rules;"""
    stmt = stmt.format('' if benchmark is None else benchmark)

    _db()
    cursor = ENGINE.raw_connection().cursor()
    schema = os.getenv('DB_SCHEMA')
    cursor.execute(f'SET search_path TO {schema}')
    cursor.execute(stmt)
    rows = [Measurement(*row) for row in cursor.fetchall()]

    # group training and test data by query
    result = {}
    for row in rows:
        if row.query_id in result:
            result[row.query_id].append(row)
        else:
            result[row.query_id] = [row]

    keys = list(result.keys())
    random.shuffle(keys)
    split_index = int(len(keys) * training_ratio)
    train_keys = keys[:split_index]
    test_keys = keys[split_index:]

    train_data = np.concatenate([result[key] for key in train_keys])
    test_data = np.concatenate([result[key] for key in test_keys])

    return train_data, test_data


def register_rule(query_path, rule, table):
    with _db() as conn:
        try:
            stmt = text(f'INSERT INTO {table} (query_id, rule) SELECT id, :rule FROM queries WHERE query_path = :query_path')
            conn.execute(stmt, rule=rule, query_path=query_path)
        except IntegrityError:
            pass  # do not store duplicates


def get_optimizers(table_name, query_path):
    with _db() as _:
        stmt = """
               SELECT optimizer_id
               FROM queries q, {0} qro
               WHERE q.query_path='{1}' AND q.id = qro.query_id
               """
        stmt = stmt.format(table_name, query_path)
        cursor = ENGINE.raw_connection().cursor()
        cursor.execute(stmt)
        return [row[0] for row in cursor.fetchall()]


def get_required_optimizers(query_path):
    return get_optimizers('query_required_optimizers', query_path)


def get_effective_optimizers(query_path):
    return get_optimizers('query_effective_optimizers', query_path)


def get_rules(table_name, query_path):
    with _db() as _:
        stmt = 'SELECT rule ' \
               'FROM queries q, {0} qro ' \
               'WHERE q.query_path=\'{1}\' AND q.id = qro.query_id'
        stmt = stmt.format(table_name, query_path)

        cursor = ENGINE.raw_connection().cursor()
        cursor.execute(stmt)
        return [row[0] for row in cursor.fetchall()]


def get_required_rules(query_path):
    return get_rules('query_required_rules', query_path)


def get_effective_rules(query_path):
    return get_rules('query_effective_rules', query_path)


def select_query(query):
    _db()
    cursor = ENGINE.raw_connection().cursor()

    schema = os.getenv('DB_SCHEMA')
    cursor.execute(f'SET search_path TO {schema}')
    cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]


def get_df(query):
    with _db() as conn:
        df = pd.read_sql(query, conn)
        return df


def register_query_config(query_path, disabled_rules, logical_dot, fragmented_dot, logical_json, fragmented_json, plan_hash):
    """
    Store the passed query optimizer configuration in the database.
    :returns: query plan is already known and a duplicate
    """
    check_for_duplicated_plans = """SELECT count(*)
        from queries q, query_optimizer_configs qoc
        where q.id = qoc.query_id
              and q.query_path = '{0}'
              and qoc.hash = {1}
              and qoc.disabled_rules != '{2}'"""
    result = select_query(check_for_duplicated_plans.format(query_path, plan_hash, disabled_rules))
    is_duplicate = result[0] > 0

    with _db() as conn:

        def literal_processor(val):
            return sqlalchemy.String('').literal_processor(dialect=ENGINE.dialect)(value=str(val))

        try:
            # created_dot_processed = literal_processor(created_dot)
            logical_dot_processed = literal_processor(logical_dot)
            fragmented_dot_processed = literal_processor(fragmented_dot)
            logical_json_processed = literal_processor(
                json.dumps(logical_json))
            fragmented_json_processed = literal_processor(
                json.dumps(fragmented_json))

            num_disabled_rules = 0 if disabled_rules is None else disabled_rules.count(',') + 1
            stmt = f"""INSERT INTO query_optimizer_configs
                   (query_id, disabled_rules, logical_plan_dot,
                   fragmented_plan_dot, logical_plan_json, fragmented_plan_json,
                    num_disabled_rules, hash, duplicated_plan) 
                   SELECT id, '{disabled_rules}', {logical_dot_processed}, {fragmented_dot_processed}, {logical_json_processed}, {fragmented_json_processed}, {num_disabled_rules}, {plan_hash}, {is_duplicate} from queries where query_path = '{query_path}'
                   """
            conn.execute(stmt)
        except IntegrityError:
            pass  # query configuration has already been inserted

    return is_duplicate


def check_for_existing_measurements(query_path, disabled_rules):
    query = f"""select count(*) as num_measurements
                from measurements m, query_optimizer_configs qoc, queries q
                where m.query_optimizer_config_id = qoc.id
                and qoc.query_id = q.id
                and q.query_path = '{query_path}'
                and qoc.disabled_rules = '{disabled_rules}'
             """
    df = get_df(query)
    values = df['num_measurements']
    return values[0] > 0


def register_measurement(query_path, disabled_rules, elapsed, planning, scheduling, running, finishing, cpu, input_data_size, nodes):
    bao_logging.info('register a new measurement for query %s and the disabled rules/optimizers [%s]', query_path, disabled_rules)
    with _db() as conn:
        now = datetime.now()
        query = f"""INSERT INTO measurements (query_optimizer_config_id, elapsed, planning, scheduling, running,
                finishing, machine, time, cpu_time, input_data_size, nodes) 
                SELECT id, {elapsed}, {planning}, {scheduling}, {running}, {finishing}, '{socket.gethostname()}', '{now.strftime('%m/%d/%Y, %H:%M:%S')}', {cpu}, {input_data_size}, {nodes} FROM query_optimizer_configs 
                WHERE query_id = (SELECT id from queries where query_path = '{query_path}') and disabled_rules = '{disabled_rules}'
                """
        conn.execute('SET datestyle = mdy;')
        conn.execute(query)


if __name__ == '__main__':
    optimizer_ids = get_required_optimizers('queries/tpch/10.sql')
    print(optimizer_ids)
    optimizer_ids = get_effective_optimizers('queries/tpch/10.sql')
    print(optimizer_ids)
    rules = get_required_rules('queries/tpch/10.sql')
    print(rules)
    rules = get_effective_rules('queries/tpch/10.sql')
    print(rules)
