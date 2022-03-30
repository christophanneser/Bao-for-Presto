#
# Copyright (C) 2020  Ryan Marcus
# Copyright (C) 2022  Christoph Anneser
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""Wrapper class for the tree convolution neural network used to implement the bandit optimizer (Bao)"""
from torch import nn
from tree_conv.tcnn import BinaryTreeConv, TreeLayerNorm
from tree_conv.tcnn import TreeActivation, DynamicPooling
from tree_conv.util import prepare_trees


def left_child(x):
    if len(x) != 3:
        return None
    return x[1]


def right_child(x):
    if len(x) != 3:
        return None
    return x[2]


def features(x):
    # assuming this is a binary tree with 3 child nodes everywhere
    if len(x) == 3:
        return x[0]
    return x


class BaoNet(nn.Module):
    """Implementation of the BaoNet neural network model"""
    def __init__(self, in_channels):
        super().__init__()
        self.__in_channels = in_channels
        self.__cuda = False

        self.tree_conv = nn.Sequential(BinaryTreeConv(self.__in_channels, 256),
                                       TreeLayerNorm(),
                                       TreeActivation(nn.LeakyReLU()),
                                       BinaryTreeConv(256, 128), TreeLayerNorm(),
                                       TreeActivation(nn.LeakyReLU()),
                                       BinaryTreeConv(128, 64),
                                       TreeLayerNorm(), DynamicPooling(),
                                       nn.Linear(64, 32), nn.LeakyReLU(),
                                       nn.Linear(32, 1))

    def in_channels(self):
        return self.__in_channels

    def forward(self, x):
        trees = prepare_trees(x,
                              features,
                              left_child,
                              right_child,
                              cuda=self.__cuda)
        return self.tree_conv(trees)

    def cuda(self):
        self.__cuda = True
        return super().cuda()
