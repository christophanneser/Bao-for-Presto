"""This module plots the relative and absolute runtime improvements wrt. the default QEP from Presto"""
from benchmark_plotter import style, texify, colors
import storage
from matplotlib.colors import LinearSegmentedColormap
import re
import os
import random
import matplotlib.pyplot as plt
import matplotlib.text as mtext
from performance_prediction import PerformancePrediction

green = '#013220'
white = '#FFFFFF'
red = '#8b1f24'
optimal_color = 'gray'  # '#32cd32'
predicted_color = colors.colors['lightblue']

colormap = LinearSegmentedColormap.from_list('Custom', [green, '#32cd32', white], N=1200)
OVERHEAD = LinearSegmentedColormap.from_list('Custom', [white, red], N=1200)
LABEL_REGEX = re.compile(r'.*/(\w*-?\w*)\.sql')
OUTPUT_DIR = 'evaluation/figures'


class LegendTitle(object):
    """Implements the legend title for a matplotlib figure"""

    def __init__(self, text_props=None):
        self.text_props = text_props or {}
        super().__init__()

    # pylint: disable=unused-argument
    def legend_artist(self, legend, orig_handle, artist, handlebox):
        x0, y0 = handlebox.xdescent, handlebox.ydescent
        title = mtext.Text(x0,
                           y0,
                           r'\textbf{' + orig_handle + '}',
                           usetex=True,
                           **self.text_props)
        handlebox.add_artist(title)
        return title


def crop_figure(filename):
    os.system(f'pdfcrop {filename} {filename}')


def process_query_label(query_path):
    return LABEL_REGEX.match(query_path).group(1)


def autolabel(axis, rects, xpos='center', color='black'):
    ha = {'center': 'center', 'right': 'left', 'left': 'right'}
    offset = {'center': 0, 'right': 0.0, 'left': 0.0}

    for rect in rects:
        textcolor = color
        height = rect.get_height()
        if height > 5000:
            height -= 4000
            textcolor = 'white'
        xy = (rect.get_x() + rect.get_width() / 2 + 0.2, height)
        axis.annotate(
            f'\\textcolor{{{textcolor}}}{{{int(height)}}}',
            xy=xy,
            xytext=(offset[xpos] * 3, 3),  # use 3 points offset
            textcoords='offset points',  # in both directions
            ha=ha[xpos],
            va='bottom',
            rotation=90)


def plot_performance(benchmark, results, experiment_type='absolute'):
    # sample a uniform subset of queries for readability
    indicies = random.sample(range(len(results)), int(0.6 * len(results)))

    # relative or absolute
    scale = -0.001 if experiment_type == 'absolute' else -100

    # sort comparator
    def get_key(entry):
        if experiment_type == 'relative':
            return scale * float(entry.savings)
        else:
            return scale * float(entry.runtime_baseline - entry.runtime)

    results = list(sorted([results[i] for i in indicies], key=get_key))
    highest, lowest = float(get_key(results[0])), float(get_key(results[-1]))

    texify.latexify(8.5, 2.3)
    style.set_custom_style()
    fig, ax = plt.subplots(nrows=1, ncols=1)

    for i, result in enumerate(results):
        if get_key(result) < 0:
            idx = int(abs(get_key(result) / highest - 1) * 1000)
            col = colormap(idx)
        else:
            idx = int(abs(get_key(result) / lowest) * 1000) + 200
            col = OVERHEAD(idx)

        ax.bar([i], [get_key(result)], width=1., linewidth=0., color=col)

    ax.set_xticks(range(len(results)))
    labels = list(map(lambda e: e.path, results))
    ax.set_xticklabels([process_query_label(path) for path in labels])
    plt.xticks(rotation=90)
    # plt.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
    ax.set_ylabel('\\textbf{{{0}}}~Runtime Changes [{1}]'.format(
        'Relative' if experiment_type == 'relative' else 'Absolute',
        '\\%' if experiment_type == 'relative' else 's'))
    ax.set_xlabel('JOB Queries')
    ax.set_xlim([-1, len(results)])

    fig.subplots_adjust(bottom=0.2)
    fig.savefig(f'{OUTPUT_DIR}/runtime_savings_{experiment_type}_{benchmark}.pgf')
    fig.savefig(f'{OUTPUT_DIR}/runtime_savings_{experiment_type}_{benchmark}.pdf')
    crop_figure(f'{OUTPUT_DIR}/runtime_savings_{experiment_type}_{benchmark}.pdf')


def plot_learned_performance(benchmark: str, test: list[PerformancePrediction], training: list[PerformancePrediction], show_training=True):
    absolute = False  # relative or absolute runtime changes wrt. default plan
    scale = -0.00001 if absolute else -100

    all_data: list[PerformancePrediction] = test + training
    all_data = sorted(all_data, key=lambda x: -x.selected_plan_relative_improvement)
    texify.latexify(6, 2.2)

    style.set_custom_style()
    fig, ax = plt.subplots(nrows=1, ncols=1)

    alpha = 0.1  # draw training set with lower alpha values as this data is already known to the TCNN model
    width = 1.0

    for i, measurement in enumerate(all_data):
        if measurement.is_training_sample:
            if not show_training:  # skip training data
                continue
            # optimal alternative hint set
            ax.bar([i],
                   scale * float(measurement.best_plan_relative_improvement),
                   width=width,
                   label='Optimal [Train]',
                   color=optimal_color,
                   alpha=alpha,
                   linewidth=0.)
            # predicted optimal plan
            ax.bar([i],
                   scale * float(measurement.selected_plan_relative_improvement),
                   width=width,
                   label='Bao [Train]',
                   color=predicted_color,
                   alpha=alpha,
                   linewidth=0.)
            if scale * float(measurement.selected_plan_relative_improvement) > 0:
                # plot the optimal training measurement
                ax.bar([i],
                       scale * float(measurement.best_plan_relative_improvement),
                       width=width,
                       label='Optimal',
                       color=optimal_color,
                       alpha=alpha,
                       linewidth=0.)
        else:

            optimal_test = ax.bar([i],
                                  scale * float(measurement.best_plan_relative_improvement),
                                  width=width,
                                  label='Optimal',
                                  color=optimal_color,
                                  alpha=None,
                                  linewidth=0.)
            predicted_test = ax.bar([i],
                                    scale * float(measurement.selected_plan_relative_improvement),
                                    width=width,
                                    label='Bao',
                                    color=predicted_color,
                                    alpha=None,
                                    linewidth=0.)
            # re-plot bars which are not visible anymore
            if scale * float(measurement.selected_plan_relative_improvement) > 0:
                optimal_test = ax.bar([i],
                                      scale * float(measurement.best_plan_relative_improvement),
                                      width=width,
                                      label='Optimal',
                                      color=optimal_color,
                                      alpha=None,
                                      linewidth=0.)

    handles = [optimal_test, predicted_test]
    ax.set_ylabel(f'''\\textbf{{{'Relative' if not absolute else 'Absolute'}}}~Runtime Changes [{'%' if not absolute else 's'}]''')
    ax.set_xlabel('Queries')
    ax.set_ylim([-44, 36])
    ax.set_xlim([-1, len(all_data)])
    ax.set_xticks([])
    ax.set_title(f'Optimal Query Optimizer Configurations ({benchmark})')
    ax.legend(handles=handles, loc='lower right')
    ax.legend(handles, ['Optimal', 'Bao'],
              handler_map={str: LegendTitle({'fontsize': 9})}, ncol=2, title=r'\textbf{Queries}',
              handlelength=0.5, columnspacing=1.5)

    plt.xticks(rotation=90)
    # ax.set_xticks(range(len(query_infos)))
    # ax.set_xticklabels(list(map(lambda entry: process_query_label(entry), query_infos)))

    # save to pdf and pgf
    experiment_type = 'rel' if not absolute else 'abs'
    # fig.savefig(f'''{OUTPUT_DIR}/runtime_savings_learned_{experiment_type}_{benchmark}{'' if show_training else '_test'}.pgf''')
    fig.savefig(f'''{OUTPUT_DIR}/runtime_savings_learned_{experiment_type}_{benchmark}{'' if show_training else '_test'}.pdf''')
    # crop_figure(f'''{OUTPUT_DIR}/runtime_savings_learned_{experiment_type}_{benchmark}.pdf''')


if __name__ == '__main__':
    for queries in ['job']:
        best_alternative_configs = storage.best_alternative_configuration(queries)
        plot_performance(queries, best_alternative_configs, 'relative')
        plot_performance(queries, best_alternative_configs, 'absolute')
