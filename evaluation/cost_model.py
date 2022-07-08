"""Evaluate if the learned model is a good cost model to predict the real runtimes of a query"""
from benchmark_plotter import style, texify
from train import deserialize_data, serialize_data, train_and_save_model, load_data
import model
from matplotlib import pyplot as plt
import numpy as np
import os


def predict_data(benchmark: list[str], data_dir, model_name, retrain=False):

    if retrain:
        x_train, y_train, x_test, y_test, training_data, test_data = load_data(benchmark, training_ratio=0.8)
        serialize_data('data', x_train, y_train, x_test, y_test, training_data, test_data)
        train_and_save_model(model_name, x_train, y_train, x_test, y_test)
    else:
        x_train, y_train, x_test, y_test, training_data, test_data = deserialize_data(data_dir)

    # load model
    bao_model = model.BaoRegression(verbose=True)
    bao_model.load(model_name)

    all_data = list(test_data) + list(training_data)

    # prepare datasets
    x = []
    y = []
    for qep in all_data:
        x.append(qep.plan_json)
        y.append(qep.running_time)

    predictions = bao_model.predict(x)

    return y, predictions


def plot_cost_model(y, predictions):
    y = [i/1_000 for i in y]
    predictions = [i/1_000 for i in predictions]

    texify.latexify(3.39, 2.0)
    style.set_custom_style()
    fig, ax = plt.subplots(nrows=1, ncols=1)

    # add a line with slope 1
    x = np.linspace(0, float(max(y)), 100)
    ax.plot(x, x, 'black')

    # actual runtime vs scatter plot
    ax.scatter(y, predictions, s=2)


    ax.set_ylabel('Predicted Runtime [s]')
    ax.set_xlabel('Actual Runtime [s]')

    plt.tight_layout()
    fig.savefig('evaluation/figures/cost_model.pdf')
    os.system('pdfcrop evaluation/figures/cost_model.pdf evaluation/figures/cost_model.pdf ')



if __name__ == '__main__':
    benchmarks = ['job']
    actual, predicted = predict_data(benchmarks, 'data', 'model', False)
    plot_cost_model(actual, predicted)
