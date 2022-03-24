"""This module implements the BaoRegression model."""
import json
import numpy as np
import torch
import torch.optim
import joblib
import os
from custom_logging import logging
from sklearn import preprocessing
from sklearn.pipeline import Pipeline
from torch.utils.data import DataLoader
import net
from featurize import TreeFeaturizer

CUDA = torch.cuda.is_available()


def _nn_path(base):
    return os.path.join(base, 'nn_weights')


def _x_transform_path(base):
    return os.path.join(base, 'x_transform')


def _y_transform_path(base):
    return os.path.join(base, 'y_transform')


def _channels_path(base):
    return os.path.join(base, 'channels')


def _n_path(base):
    return os.path.join(base, 'n')


def _inv_log1p(x):
    return np.exp(x) - 1


class BaoData:
    def __init__(self, data):
        assert data
        self.__data = data

    def __len__(self):
        return len(self.__data)

    def __getitem__(self, idx):
        return self.__data[idx]['tree'], self.__data[idx]['target']


def collate(x):
    trees = []
    targets = []

    for tree, target in x:
        trees.append(tree)
        targets.append(target)

    targets = torch.tensor(targets)
    return trees, targets


class BaoRegression:
    """This class represents the Bao regression model used to predict execution times of query plans"""

    def __init__(self, verbose=False, have_cache_data=False):
        self.__net = None
        self.__verbose = verbose

        log_transformer = preprocessing.FunctionTransformer(np.log1p, _inv_log1p, validate=True)
        scale_transformer = preprocessing.MinMaxScaler()

        self.__pipeline = Pipeline([('log', log_transformer), ('scale', scale_transformer)])

        self.__tree_transform = TreeFeaturizer()
        self.__have_cache_data = have_cache_data
        self.__in_channels = None
        self.__n = 0
        self.__n_test = 0

    def __log(self, *args):
        if self.__verbose:
            logging.info(*args)

    def num_items_trained_on(self):
        return self.__n

    def load(self, path):
        self.__log('Load Bao regression model from directory %s ...', path)
        with open(_n_path(path), 'rb') as f:
            self.__n = joblib.load(f)
        with open(_channels_path(path), 'rb') as f:
            self.__in_channels = joblib.load(f)

        self.__net = net.BaoNet(self.__in_channels)
        self.__net.load_state_dict(torch.load(_nn_path(path)))
        self.__net.eval()

        with open(_y_transform_path(path), 'rb') as f:
            self.__pipeline = joblib.load(f)
        with open(_x_transform_path(path), 'rb') as f:
            self.__tree_transform = joblib.load(f)

    def save(self, path):
        # try to create a directory here
        os.makedirs(path, exist_ok=True)

        torch.save(self.__net.state_dict(), _nn_path(path))
        with open(_y_transform_path(path), 'wb') as f:
            joblib.dump(self.__pipeline, f)
        with open(_x_transform_path(path), 'wb') as f:
            joblib.dump(self.__tree_transform, f)
        with open(_channels_path(path), 'wb') as f:
            joblib.dump(self.__in_channels, f)
        with open(_n_path(path), 'wb') as f:
            joblib.dump(self.__n, f)

    def fit(self, x_train, y_train, x_test, y_test):
        if isinstance(y_train, list):
            y_train = np.array(y_train)

        if isinstance(y_test, list):
            y_test = np.array(y_test)

        x_train = [json.loads(x) if isinstance(x, str) else x for x in x_train]
        x_test = [json.loads(x) if isinstance(x, str) else x for x in x_test]

        self.__n = len(x_train)
        self.__n_test = len(x_test)

        # transform the set of trees into feature vectors using a log
        # (assuming the tail behavior exists, TODO investigate
        #  the quantile transformer from scikit)
        y_train = self.__pipeline.fit_transform(y_train.reshape(-1, 1)).astype(np.float32)
        y_test = self.__pipeline.fit_transform(y_test.reshape(-1, 1)).astype(np.float32)

        self.__tree_transform.fit(x_train + x_test)
        x_train = self.__tree_transform.transform(x_train)
        x_test = self.__tree_transform.transform(x_test)

        pairs = list(zip(x_train, y_train))
        pairs_test = list(zip(x_test, y_test))

        dataset_train = DataLoader(pairs, batch_size=16, shuffle=True, collate_fn=collate)
        dataset_test = DataLoader(pairs_test, batch_size=16, shuffle=True, collate_fn=collate)

        # determine the initial number of channels
        for inp, _ in dataset_train:
            in_channels = inp[0][0].shape[0]
            break

        self.__log('Initial input channels: %s', in_channels)

        # if self.__have_cache_data: removed

        self.__net = net.BaoNet(in_channels)
        self.__in_channels = in_channels
        if CUDA:
            self.__net = self.__net.cuda()

        optimizer = torch.optim.Adam(self.__net.parameters())
        loss_fn = torch.nn.MSELoss()

        training_losses = []
        test_losses = []
        for epoch in range(10):
            training_loss_accum = 0
            test_loss_accum = 0
            for x, y in dataset_train:
                if CUDA:
                    y = y.cuda()
                y_pred = self.__net(x)
                loss = loss_fn(y_pred, y)
                training_loss_accum += loss.item()

                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            for x, y in dataset_test:
                if CUDA:
                    y = y.cuda()
                y_pred = self.__net(x)
                test_loss_accum += loss_fn(y_pred, y).item()

            training_loss_accum /= len(dataset_train)
            test_loss_accum /= len(dataset_test)
            training_losses.append(training_loss_accum)
            test_losses.append(test_loss_accum)
            if epoch % 1 == 0:
                self.__log('Epoch %s\ttrain. loss\t%.4f', epoch, training_loss_accum)
                self.__log('Epoch %s\ttest loss\t%.4f', epoch, test_loss_accum)

            # stopping condition
            if len(training_losses) > 10 and training_losses[-1] < 0.1:
                last_two = np.min(training_losses[-2:])
                if last_two > training_losses[-10] or (training_losses[-10] - last_two < 0.0001):
                    self.__log('Stopped training from convergence condition at epoch', epoch)
                    break
        else:
            self.__log('Stopped training after max epochs')
        return training_losses, test_losses

    def predict(self, x):
        """Predict one or more samples"""
        if not isinstance(x, list):
            x = [x]  # x represents one sample only
        x = [json.loads(x) if isinstance(x, str) else x for x in x]

        x = self.__tree_transform.transform(x)

        self.__net.eval()
        prediction = self.__net(x).cpu().detach().numpy()
        return self.__pipeline.inverse_transform(prediction)
