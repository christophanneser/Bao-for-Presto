"""This module trains and evaluates BAO"""
import os
import storage
import model
import numpy as np
import pickle
from custom_logging import bao_logging


class BaoTrainingException(Exception):
    pass


def load_data(benchmark=None, training_ratio=0.8):
    training_data, test_data = storage.experience(benchmark, training_ratio)

    x_train = [config.plan_json for config in training_data]
    y_train = [config.running_time for config in training_data]
    x_test = [config.plan_json for config in test_data]
    y_test = [config.running_time for config in test_data]

    return x_train, y_train, x_test, y_test, training_data, test_data


def serialize_data(directory, x_train, y_train, x_test, y_test, training_configs, test_configs):
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open('{0}/x_train'.format(directory), 'wb') as f:
        pickle.dump(x_train, f, pickle.HIGHEST_PROTOCOL)
    with open('{0}/y_train'.format(directory), 'wb') as f:
        pickle.dump(y_train, f, pickle.HIGHEST_PROTOCOL)
    with open('{0}/x_test'.format(directory), 'wb') as f:
        pickle.dump(x_test, f, pickle.HIGHEST_PROTOCOL)
    with open('{0}/y_test'.format(directory), 'wb') as f:
        pickle.dump(y_test, f, pickle.HIGHEST_PROTOCOL)
    with open('{0}/training_configs'.format(directory), 'wb') as f:
        pickle.dump(training_configs, f, pickle.HIGHEST_PROTOCOL)
    with open('{0}/test_configs'.format(directory), 'wb') as f:
        pickle.dump(test_configs, f, pickle.HIGHEST_PROTOCOL)


def deserialize_data(directory):
    with open('{0}/x_train'.format(directory), 'rb') as f:
        x_train = pickle.load(f)
    with open('{0}/y_train'.format(directory), 'rb') as f:
        y_train = pickle.load(f)
    with open('{0}/x_test'.format(directory), 'rb') as f:
        x_test = pickle.load(f)
    with open('{0}/y_test'.format(directory), 'rb') as f:
        y_test = pickle.load(f)
    with open('{0}/training_configs'.format(directory), 'rb') as f:
        training_configs = pickle.load(f)
    with open('{0}/test_configs'.format(directory), 'rb') as f:
        test_configs = pickle.load(f)
    return x_train, y_train, x_test, y_test, training_configs, test_configs


def train_and_save_model(filename, x_train, y_train, x_test, y_test, verbose=True):
    bao_logging.info('training samples: %s, test samples: %s', len(x_train), len(x_test))

    if not x_train:
        raise BaoTrainingException('Cannot train a Bao model with no experience')

    if len(x_train) < 20:
        bao_logging.warning('Warning: trying to train a Bao model with fewer than 20 datapoints.')

    regression_model = model.BaoRegression(have_cache_data=True, verbose=verbose)
    losses = regression_model.fit(x_train, y_train, x_test, y_test)
    regression_model.save(filename)

    return regression_model, losses


def evaluate_prediction(y, prediction, plans):
    default_plan = list(filter(lambda x: x.num_disabled_rules == 0, plans))[0]

    bao_logging.info('y:\t%s', '\t'.join(['{:.2f}'.format(_) for _ in y]))
    bao_logging.info('ŷ:\t%s', '\t'.join('{:.2f}'.format(_[0]) for _ in prediction))
    min_prediction_index = np.argmin(prediction)
    bao_logging.info('min pred index: %s', str(min_prediction_index))

    # evaluate performance gains
    performance_from_model = y[min_prediction_index]
    bao_logging.info('best choice -> %s', str(y[0] / default_plan.running_time))

    if performance_from_model < default_plan.running_time:
        bao_logging.info('good choice -> %s', str(performance_from_model / default_plan.running_time))
    else:
        bao_logging.info('bad choice -> %s', str(performance_from_model / default_plan.running_time))

    return [(default_plan.running_time - y[0]) / default_plan.running_time,
            (default_plan.running_time - y[min_prediction_index]) /
            default_plan.running_time, default_plan.running_time - y[0],
            default_plan.running_time - y[min_prediction_index],
            default_plan.running_time]


def choose_best_plans(filename, test_configs):
    # load model
    bao_model = model.BaoRegression(have_cache_data=True, verbose=True)
    bao_model.load(filename)

    # load query plans for prediction
    all_query_plans = dict()
    for plan_runtime in test_configs:
        if plan_runtime.query_id in all_query_plans:
            all_query_plans[plan_runtime.query_id].append(plan_runtime)
        else:
            all_query_plans[plan_runtime.query_id] = [plan_runtime]

    performance = []

    for query_id in sorted(all_query_plans.keys()):
        plans_and_estimates = all_query_plans[query_id]
        plans_and_estimates = sorted(plans_and_estimates, key=lambda record: record.running_time)

        bao_logging.info('Preprocess data for query %s', plans_and_estimates[0].query_path)
        x = [x.plan_json for x in plans_and_estimates]
        y = [x.running_time for x in plans_and_estimates]

        predictions = bao_model.predict(x)
        actual, predicted, actual_abs_improve, predicated_abs_improve, default_abs = evaluate_prediction(y, predictions, plans_and_estimates)
        performance.append((actual, predicted, actual_abs_improve, predicated_abs_improve, default_abs, plans_and_estimates[0].query_path))
    return reversed(sorted(performance, key=lambda entry: entry[0]))


def train():
    benchmark = 'tpch'
    retrain = True

    if retrain:
        x_train, y_train, x_test, y_test, training_data, test_data = load_data(benchmark=benchmark, training_ratio=0.8)
        serialize_data('data', x_train, y_train, x_test, y_test, training_data, test_data)
        train_and_save_model('model', x_train, y_train, x_test, y_test)
    else:
        x_train, y_train, x_test, y_test, training_data, test_data = deserialize_data('data')

    performance_test = list(choose_best_plans('model', test_data))
    # actual_test = list(map(lambda e: float(e[0]), performance_test))
    # learned_test = list(map(lambda e: float(e[1]), performance_test))
    # query_infos_test = list(map(lambda e: e[2], performance_test))

    performance_training = list(choose_best_plans('model', training_data))
    # actual_test = list(map(lambda e: float(e[0]), performance_training))
    # learned_test = list(map(lambda e: float(e[1]), performance_training))
    # query_infos_test = list(map(lambda e: e[2], performance_training))

    # calculate absolute improvements
    abs_improv_test = sum([x[3] for x in performance_test])
    abs_test = sum([x[4] for x in performance_test])
    print('test improvement rel: {:.4f}'.format(abs_improv_test / abs_test))

    abs_improv_test = sum([x[3] for x in performance_training])
    abs_test = sum([x[4] for x in performance_training])
    print('training improvement rel: {:.4f}'.format(abs_improv_test / abs_test))


if __name__ == '__main__':
    train()
