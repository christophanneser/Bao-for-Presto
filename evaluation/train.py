"""This module trains and evaluates the Bao integration for Presto"""
import os
import storage
import model
import numpy as np
import pickle
from custom_logging import bao_logging
from plot_optimizer_configs import plot_learned_performance
from performance_prediction import PerformancePrediction


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
    with open(f'{directory}/x_train', 'wb') as f:
        pickle.dump(x_train, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/y_train', 'wb') as f:
        pickle.dump(y_train, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/x_test', 'wb') as f:
        pickle.dump(x_test, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/y_test', 'wb') as f:
        pickle.dump(y_test, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/training_configs', 'wb') as f:
        pickle.dump(training_configs, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/test_configs', 'wb') as f:
        pickle.dump(test_configs, f, pickle.HIGHEST_PROTOCOL)


def deserialize_data(directory):
    with open(f'{directory}/x_train', 'rb') as f:
        x_train = pickle.load(f)
    with open(f'{directory}/y_train', 'rb') as f:
        y_train = pickle.load(f)
    with open(f'{directory}/x_test', 'rb') as f:
        x_test = pickle.load(f)
    with open(f'{directory}/y_test', 'rb') as f:
        y_test = pickle.load(f)
    with open(f'{directory}/training_configs', 'rb') as f:
        training_configs = pickle.load(f)
    with open(f'{directory}/test_configs', 'rb') as f:
        test_configs = pickle.load(f)
    return x_train, y_train, x_test, y_test, training_configs, test_configs


def train_and_save_model(filename, x_train, y_train, x_test, y_test, verbose=True):
    bao_logging.info('training samples: %s, test samples: %s', len(x_train), len(x_test))

    if not x_train:
        raise BaoTrainingException('Cannot train a Bao model with no experience')

    if len(x_train) < 20:
        bao_logging.warning('Warning: trying to train a Bao model with fewer than 20 datapoints.')

    regression_model = model.BaoRegression(verbose=verbose)
    losses = regression_model.fit(x_train, y_train, x_test, y_test)
    regression_model.save(filename)

    return regression_model, losses


def evaluate_prediction(y, predictions, plans, query_path, is_training) -> PerformancePrediction:
    default_plan = list(filter(lambda x: x.num_disabled_rules == 0, plans))[0]

    bao_logging.info('y:\t%s', '\t'.join([f'{_:.2f}' for _ in y]))
    bao_logging.info('ŷ:\t%s', '\t'.join(f'{prediction[0]:.2f}' for prediction in predictions))
    # the plan index which is estimated to perform best by Bao
    min_prediction_index = np.argmin(predictions)
    bao_logging.info('min predicted index: %s (smaller is better)', str(min_prediction_index))

    # evaluate performance gains with Bao
    performance_from_model = y[min_prediction_index]
    bao_logging.info('best choice -> %s', str(y[0] / default_plan.running_time))

    if performance_from_model < default_plan.running_time:
        bao_logging.info('good choice -> %s', str(performance_from_model / default_plan.running_time))
    else:
        bao_logging.info('bad choice -> %s', str(performance_from_model / default_plan.running_time))

    return PerformancePrediction(default_plan.running_time, plans[min_prediction_index].running_time, plans[0].running_time, query_path, is_training)


def choose_best_plans(filename: str, test_configs: list[storage.Measurement], is_training: bool) -> list[PerformancePrediction]:
    """For each query, let Bao estimate the performance of all QEPs and compare them to the runtime of the default plan"""

    # load model
    bao_model = model.BaoRegression(verbose=True)
    bao_model.load(filename)

    # load query plans for prediction
    all_query_plans = {}
    for plan_runtime in test_configs:
        if plan_runtime.query_id in all_query_plans:
            all_query_plans[plan_runtime.query_id].append(plan_runtime)
        else:
            all_query_plans[plan_runtime.query_id] = [plan_runtime]

    performance_predictions: list[PerformancePrediction] = []

    for query_id in sorted(all_query_plans.keys()):
        plans_and_estimates = all_query_plans[query_id]
        plans_and_estimates = sorted(plans_and_estimates, key=lambda record: record.running_time)
        query_path = plans_and_estimates[0].query_path

        bao_logging.info('Preprocess data for query %s', plans_and_estimates[0].query_path)
        x = [x.plan_json for x in plans_and_estimates]
        y = [x.running_time for x in plans_and_estimates]

        predictions = bao_model.predict(x)
        performance_prediction = evaluate_prediction(y, predictions, plans_and_estimates, query_path, is_training)
        performance_predictions.append(performance_prediction)
    return list(reversed(sorted(performance_predictions, key=lambda entry: entry.selected_plan_relative_improvement)))


def train():
    benchmark = 'job'
    retrain = False

    if retrain:
        x_train, y_train, x_test, y_test, training_data, test_data = load_data(benchmark=benchmark, training_ratio=0.8)
        serialize_data('data', x_train, y_train, x_test, y_test, training_data, test_data)
        train_and_save_model('model', x_train, y_train, x_test, y_test)
    else:
        x_train, y_train, x_test, y_test, training_data, test_data = deserialize_data('data')

    performance_test = choose_best_plans('model', test_data, is_training=False)
    # actual_test = list(map(lambda e: float(e[0]), performance_test))
    # learned_test = list(map(lambda e: float(e[1]), performance_test))
    # query_infos_test = list(map(lambda e: e[2], performance_test))

    performance_training = choose_best_plans('model', training_data, is_training=True)
    # actual_test = list(map(lambda e: float(e[0]), performance_training))
    # learned_test = list(map(lambda e: float(e[1]), performance_training))
    # query_infos_test = list(map(lambda e: e[2], performance_training))

    # calculate absolute improvements
    abs_improvements_test = sum([x.selected_plan_absolute_improvement for x in performance_test])
    abs_test = sum([x.default_plan_runtime for x in performance_test])
    print(f'test improvement rel: {(abs_improvements_test / abs_test):.4f}')

    abs_improvements_test = sum([x.selected_plan_absolute_improvement for x in performance_training])
    abs_test = sum([x.default_plan_runtime for x in performance_training])
    print(f'training improvement rel: {(abs_improvements_test / abs_test):.4f}')

    # plot_learned_performance('JOB', performance_test, performance_training, show_training=False)
    plot_learned_performance('JOB', performance_test, performance_training, show_training=True)


if __name__ == '__main__':
    train()
