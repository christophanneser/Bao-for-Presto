"""Visualize the impact of different hint set on the performance using boxplots"""
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import statistics
from benchmark_plotter import style, texify, colors, utils
import pandasql as pdsql
import storage

if __name__ == '__main__':

    # ===== set matplotlib latex style =====
    queries = storage.get_df("""select * from queries where query_path like '%%job%%'""")
    for query in queries['query_path']:
        texify.latexify(3.39, 2.2, custom_params={'xtick.labelsize': 5})
        style.set_custom_style()

        steps = f''' select count(distinct num_disabled_rules) as num_disabled_rules
            from queries q, query_optimizer_configs qoc
            where qoc.query_id = q.id and q.query_path = '{query}';
            '''
        steps_df = storage.get_df(steps)
        test = storage.get_df('select count(*) from measurements;')
        steps = steps_df['num_disabled_rules'].tolist()[0]

        with PdfPages(f'''evaluation/figures/optimizer_configs/{query.split('/')[-1].replace('.sql', '.pdf')}''') as pdf:
            for num_disabled_rules in range(1, 2):
                fig, ax = plt.subplots(nrows=1, ncols=1)

                benchmark = ''
                stmt = f'''
                    select q.query_path, (running + finishing) as elapsed, qoc.disabled_rules, m.time
                    from queries q,
                         measurements m,
                         query_optimizer_configs qoc
                    where m.query_optimizer_config_id = qoc.id
                      and qoc.query_id = q.id
                      and q.query_path = '{query}'
                      and (num_disabled_rules = {num_disabled_rules} or num_disabled_rules = 0)
                    order by m.time asc;
                    '''
                df = storage.get_df(stmt)
                disabled_rules = pdsql.sqldf(
                    'select distinct disabled_rules from df', locals())
                runtimes = {}
                for dr in disabled_rules['disabled_rules']:
                    runs = pdsql.sqldf(f'''select elapsed from df where disabled_rules = '{dr}' order by time asc''')
                    # drop the first measurement of each query
                    data = runs['elapsed'].to_list()[1:]  # list(filter(lambda x: x < 400000, runs['elapsed'].to_list()[1:]))  #  #
                    if len(data) > 0:
                        if dr == 'None':
                            data = data[:15]
                        runtimes[dr] = data
                    pass

                values = list(runtimes.values())
                keys = list(runtimes.keys())

                measurements = [[keys[i], [val / 1000 for val in values[i]]] for i in range(len(values))]
                measurements = sorted(measurements, key=lambda x: statistics.median(x[1]))

                max_tick_length = max([len(x[0]) for x in measurements])
                # plt.subplots_adjust(bottom=0.0125 * max_tick_length)
                texify.latexify(3.39, 3 + 0.15 * max_tick_length)
                plt.subplots_adjust(bottom=0.2 + 0.003 * max_tick_length)

                flierprops = dict(marker='o', markerfacecolor='black', markersize=2, linestyle='none', markeredgecolor='black')
                ax.boxplot([x[1] for x in measurements], flierprops=flierprops)
                ax.set_xticklabels(utils.shorten_optimizer_config(x[0]) for x in measurements)
                ax.set_ylabel('execution time [s]')

                plt.xticks(rotation=90)

                # draw median of default measurement
                default_measurements = runtimes['None']
                med = statistics.median(default_measurements) / 1000
                plt.axhline(y=med, color=colors.colors['blue'], linestyle='--', linewidth=1)

                # colorize space around None measurement
                index = -1
                for i in range(len(measurements)):
                    if measurements[i][0] == 'None':
                        index = i
                        break
                ax.axvspan(index + 0.5, index + 1.5, color=colors.colors['blue'], alpha=0.1, linewidth=0)
                # ax.set_ylim([0, 4.5])
                # ax.set_title(f'{query} - {title}') #DP Stage {num_disabled_rules + 1}\n
                ax.text(0.3, 1.0, f'{query}', transform=ax.transAxes, fontsize=10, alpha=0.3)
                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)
