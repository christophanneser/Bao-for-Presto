"""This module defines the basic settings of the plots. Use LaTex for typesetting."""
import seaborn as sns
import matplotlib as mpl


def set_custom_style():
    sns.set_style('darkgrid', {'axes.linewidth': 2,
                               'axes.edgecolor': 'black',
                               'xtick.bottom': True,
                               'ytick.left': True,
                               'grid.linestyle': '--',
                               'grid.alpha': 0.1,
                               'grid.linewidth': 0.1,
                               'axes.facecolor': 'white',
                               'grid.color': 'lightgray',
                               'axes.spines.left': True,
                               'axes.spines.bottom': True,
                               'axes.spines.right': False,
                               'axes.spines.top': False,
                               })

    color_cycle = ['#377eba', '#a10628', '#984ea3', '#ff7f00', '#4daf4a', '#f781bf', '#999999', '#e41a1c', '#dede00']
    mpl.rcParams['axes.prop_cycle'] = mpl.cycler(color=color_cycle)
