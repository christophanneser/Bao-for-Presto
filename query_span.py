"""This module implements a generic but naive approach to approximate the query span. A system integration will be much more efficient."""
import queue
from multiprocessing import Pool
import prestodb
import numpy as np
import time
import os

import storage

N_THREADS = 16
FAILED = 'failed'


class Optimizer:
    def __init__(self, name, dependencies):
        self.name: str = name
        self.dependencies: Optimizer = dependencies
        self.result = None
        self.required = False

    def __str__(self):
        res = '' if self.dependencies is None else (',' + str(self.dependencies))
        return self.name + res


def _presto_get_query_plan(args: tuple) -> Optimizer:
    sql_query: str = args[0]
    optimizer: Optimizer = args[1]

    disabled_optimizers = '' if optimizer is None else str(optimizer)
    # for presto, we can disable rules via the session properties directly here
    session_properties = {'bao_disabled_optimizers': disabled_optimizers}

    connection = prestodb.dbapi.connect(
        host='localhost',
        port=8080,
        user='admin',
        catalog='hive',
        schema='imdb',
        request_timeout=60,
        session_properties=session_properties
    )

    cursor = connection.cursor()
    cursor.execute(f'explain (format json) {sql_query}')
    try:
        result = cursor.fetchall()
        optimizer.result = result[0][0]
    except prestodb.exceptions.PrestoQueryError:
        optimizer.result = FAILED
    connection.close()
    return optimizer


def _unpack(arg, function):
    return function(*arg)


def approximate_query_span(sql_query: str, knobs: np.array, get_json_query_plan, find_alternative_rules=False, batch_wise=False) -> list[Optimizer]:
    knobs = np.array([Optimizer(knob, None) for knob in knobs])
    with Pool(N_THREADS) as thread_pool:
        query_span: list[Optimizer] = []
        default_plan = get_json_query_plan((sql_query, Optimizer('', None)))

        args = [(sql_query, knob) for knob in knobs]
        results = np.array(thread_pool.map(get_json_query_plan, args))

        default_plan_hash = hash(default_plan.result)
        failed_plan_hash = hash(FAILED)

        hashes = np.array(list(map(lambda res: hash(res.result), results)))
        effective_optimizers_indexes = np.where((hashes != default_plan_hash) & (hashes != failed_plan_hash))
        required_optimizers_indexes = np.where(hashes == failed_plan_hash)

        new_effective_optimizers = queue.Queue()
        for optimizer in results[effective_optimizers_indexes]:
            new_effective_optimizers.put(optimizer)

        required_optimizers = results[required_optimizers_indexes]
        for required_optimizer in required_optimizers:
            required_optimizer.required = True
            query_span.append(required_optimizer)

        # note that indices change after the delete
        knobs = np.delete(knobs, np.concatenate([effective_optimizers_indexes, required_optimizers_indexes], axis=1))

        if find_alternative_rules:
            if batch_wise:
                # disable all optimizers at once an re-run query optimization (this is Pari's approach)
                found_new_optimizers = True
                all_effective_optimizers = Optimizer(','.join(map(lambda opt: opt.name, results[effective_optimizers_indexes])), None)

                while found_new_optimizers:
                    for optimizer in results[effective_optimizers_indexes]:
                        query_span.append(optimizer)
                    default_plan = get_json_query_plan((sql_query, all_effective_optimizers))
                    default_plan_hash = hash(default_plan.result)
                    args = [(sql_query, Optimizer(knob.name, all_effective_optimizers)) for knob in knobs]
                    results = np.array(thread_pool.map(get_json_query_plan, args))
                    hashes = np.array(list(map(lambda res: hash(res.result), results)))
                    effective_optimizers_indexes = np.where((hashes != default_plan_hash) & (hashes != failed_plan_hash))
                    new_alternative_optimizers = results[effective_optimizers_indexes]
                    for new_alternative_optimizer in new_alternative_optimizers:
                        all_effective_optimizers.name += ',' + new_alternative_optimizer.name
                    found_new_optimizers = len(effective_optimizers_indexes[0]) > 0

            else:
                while not new_effective_optimizers.empty():
                    effective_optimizer = new_effective_optimizers.get()
                    query_span.append(effective_optimizer)
                    default_plan_hash = hash(effective_optimizer.result)
                    args = [(sql_query, Optimizer(knob.name, effective_optimizer)) for knob in knobs]
                    results = np.array(thread_pool.map(get_json_query_plan, args))
                    hashes = np.array(list(map(lambda res: hash(res.result), results)))
                    effective_optimizers_indexes = np.where((hashes != default_plan_hash) & (hashes != failed_plan_hash))
                    # required_optimizers_indexes = np.where(results == failed_plan_hash)

                    new_alternative_optimizers = results[effective_optimizers_indexes]

                    # add new alternative optimizers to the queue, remove them from the knobs
                    for alternative_optimizer in new_alternative_optimizers:
                        new_effective_optimizers.put(alternative_optimizer)
                        for i in reversed(range(len(knobs))):
                            if knobs[i].name == alternative_optimizer.name:
                                np.delete(knobs, i)
                                break

                    if len(new_alternative_optimizers) > 0:
                        for opt in new_alternative_optimizers:
                            print(opt)
                        print('detected new alternative rules...')
                    else:
                        print('could not find alternative rules...')
        else:
            while not new_effective_optimizers.empty():
                new_effective_optimizer = new_effective_optimizers.get()
                query_span.append(new_effective_optimizer)
    return query_span


def start_presto():
    print('start presto...')
    os.system('cd ~/presto/driver/Bao-Presto-Integration/Docker/presto-server-0.274-SNAPSHOT/ && python3 bin/launcher.py start')


def restart_presto():
    print('restart presto...')
    os.system('cd ~/presto/driver/Bao-Presto-Integration/Docker/presto-server-0.274-SNAPSHOT/ && python3 bin/launcher.py restart')


if __name__ == '__main__':
    start_presto()
    time.sleep(15)
    # Benchmark for Presto and PostgreSQL and approximation of QuerySpans
    with open('./evaluation/query_span_approximation.csv', 'a', encoding='utf-8') as results_file:
        query_files = os.listdir('./queries/job/')
        for filename in ['2c.sql']:
            query = storage.read_sql_file(f'./queries/job/{filename}')

            # restart presto for each query

            with open('knobs/presto.txt', 'r', encoding='utf-8') as file:
                db_knobs = file.readlines()
                db_knobs = np.array([knob.replace('\n', '') for knob in db_knobs])

            for find_alternatives in [True]:
                start = time.time()
                span = approximate_query_span(query, db_knobs, _presto_get_query_plan, find_alternative_rules=find_alternatives, batch_wise=True)
                end = time.time()
                print(end - start)
                required_opts = list(filter(lambda opt: opt.required, span))
                effective_opts = list(filter(lambda opt: not opt.required and opt.dependencies is None, span))
                alternative_ops = list(filter(lambda opt: not opt.required and opt.dependencies is not None, span))
                results_file.write(
                    f'job/{filename},{find_alternatives},{end - start},{len(effective_opts)},{len(required_opts)},{len(alternative_ops)}\n')

            restart_presto()
            time.sleep(15)
