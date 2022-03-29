"""This module provides the classes QuerySpan and the OptimizerConfiguration"""
import storage
import pandasql as pdsql
import statistics
import progressbar
from custom_logging import bao_logging
from presto_query_optimizer import always_required_optimizers, always_required_rules
from session_properties import BAO_DISABLED_OPTIMIZERS, BAO_DISABLED_RULES

# define early stopping and do not explore all possible optimizer configurations
MAX_DP_DEPTH = 2


def tuple_to_list(t):
    if len(t) == 1:
        return [t[0]]
    else:
        return list(t)


class QuerySpan:
    """This implementation is specific to presto, as it differentiates rules and optimizers"""

    def __init__(self):
        self.effective_rules = None
        self.effective_optimizers = None
        self.required_rules = None
        self.required_optimizers = None

    def get_tunable_optimizers(self):
        return sorted(list(set(self.effective_optimizers).difference(self.required_optimizers, always_required_optimizers)))

    def get_tunable_rules(self):
        return sorted(list(set(self.effective_rules).difference(self.required_rules, always_required_rules)))


class OptimizerConfig:
    """An OptimizerConfig allows to efficiently explore the search space of different optimizer settings.
      It implements a dynamic programming-based approach to execute promising optimizer configurations (e.g. disable certain optimizers)"""

    def __init__(self, query_path):
        self.query_path = query_path
        # store configs that resulted in runtimes worse than the baseline
        self.blacklisted_configs = set()

        query_span = QuerySpan()
        query_span.required_optimizers = storage.get_required_optimizers(self.query_path)
        query_span.effective_optimizers = storage.get_effective_optimizers(self.query_path)
        query_span.required_rules = storage.get_required_rules(self.query_path)
        query_span.effective_rules = storage.get_effective_rules(self.query_path)
        self.query_span = query_span
        self.tunable_opts_rules = query_span.get_tunable_optimizers() + query_span.get_tunable_rules()

        self.n = 0  # consider 1 rule/optimizer at once
        self.configs = self.get_next_configs()
        self.iterator = -1

        self.progress_bar = None
        self.restart_progress_bar()
        print('\trun {0} different configs'.format(self.get_num_configs()))

    def dp_combine(self, promising_disabled_opts, previous_configs):
        result = set()
        # based on previous results, use DP to build new interesting configurations
        for optimizer in promising_disabled_opts:
            # combine with all other result
            for conf in previous_configs:
                if optimizer[0] not in conf:
                    new_config = frozenset(conf + optimizer)
                    execute_config = True
                    for bad_config in self.blacklisted_configs:
                        if bad_config.issubset(new_config):
                            execute_config = False
                            break
                    if execute_config:
                        result.add(frozenset(conf + optimizer))
        return sorted([sorted(list(x)) for x in result])  # key=lambda x: ''.join(x)

    def restart_progress_bar(self):
        if self.progress_bar is not None:
            self.progress_bar.finish()
        self.progress_bar = progressbar.ProgressBar(
            maxval=self.get_num_configs(),
            widgets=[progressbar.Bar('=', '[', ']'), ' '])
        self.progress_bar.start()

    def __repr__(self):
        return 'Config {{\n\toptimizers:{0},\n\trules:{1}}}'.format(
            self.tunable_optimizers, self.editable_rules)

    def get_measurements(self):
        # we do not consider planning and scheduling time in the total runtime
        stmt = '''
            select (running + finishing) as total_runtime, qoc.disabled_rules, m.time, qoc.num_disabled_rules
            from queries q,
                 measurements m,
                 query_optimizer_configs qoc
            where m.query_optimizer_config_id = qoc.id
              and qoc.query_id = q.id
              and q.query_path = '{0}'
              and (qoc.num_disabled_rules = 1 or qoc.duplicated_plan = false)
            order by m.time asc;
            '''.format(self.query_path)
        df = storage.get_df(stmt)
        return df

    def get_baseline(self):
        # pylint: disable=possibly-unused-variable
        df = self.get_measurements()
        runs = pdsql.sqldf('select total_runtime from df where disabled_rules = \'None\'', locals())
        runtimes = runs['total_runtime'].to_list()
        return runtimes

    def get_promising_measurements_by_num_rules(self, num_disabled_rules,
                                                baseline_median,
                                                baseline_mean):
        measurements = self.get_measurements()
        stmt = '''select total_runtime, disabled_rules, time
        from measurements
        where num_disabled_rules = {0};
        '''.format(num_disabled_rules)
        df = pdsql.sqldf(stmt, locals())
        measurements = df.groupby(['disabled_rules'
                                   ])['total_runtime'].agg(['median', 'mean'])

        # find bad configs and black list them so they are not used in later DP stages
        bad_configs = measurements[(measurements['median'] > baseline_median) |
                                   (measurements['mean'] > baseline_mean)]
        for config in bad_configs.index.values.tolist():
            opts = config.split(',')
            self.blacklisted_configs.add(frozenset(opts))

        # find good configs which are better than the default config with all optimizers enabled
        good_configs = measurements[(measurements['median'] < baseline_median)
                                    & (measurements['mean'] <= baseline_mean)]

        configs = good_configs.index.values.tolist()
        configs = filter(lambda n: n != 'None', configs)
        return [conf.split(',') for conf in configs]

    # create the next configs starting with one disabled optimizer and then, switch to dynamic programming
    def get_next_configs(self):
        n = self.n
        if n > len(self.tunable_opts_rules) or n > MAX_DP_DEPTH:
            return None
        elif n == 0:
            configs = [[]]
        elif n == 1:
            configs = [[opt] for opt in self.tunable_opts_rules]
        else:
            # build config based on DP
            baseline = self.get_baseline()
            try:
                # basic statistics for baseline
                median = statistics.median(baseline)
                mean = statistics.mean(baseline)
                # get results from previous runs, consider only those configs better than the baseline
                single_optimizers = self.get_promising_measurements_by_num_rules(1, median, mean)
                combinations_previous_run = self.get_promising_measurements_by_num_rules(n - 1, median, mean)
                # use configs from n-1 and combine with n=1
                configs = self.dp_combine(single_optimizers, combinations_previous_run)
            except ArithmeticError as err:
                bao_logging.info('DP: get_next_configs() results in an ArithmeticError %s', err)
                configs = None
        self.n += 1
        return configs

    def get_num_configs(self):
        return len(self.configs)

    def get_disabled_opts_rules(self):
        if self.configs is None or len(self.configs) == 0 or len(self.configs[self.iterator]) == 0:
            return None
        return ','.join(sorted(tuple_to_list(self.configs[self.iterator])))

    def has_next(self):
        if self.iterator < self.get_num_configs() - 1:
            return True
        self.configs = self.get_next_configs()
        if self.configs is None:
            return False
        bao_logging.info('Enter next DP stage, run %s configurations', len(self.configs))
        self.restart_progress_bar()
        self.iterator = -1
        return self.iterator < self.get_num_configs() - 1

    def next(self):
        self.iterator += 1
        self.progress_bar.update(self.iterator)
        conf = self.configs[self.iterator]
        tmp_optimizers = list(filter(lambda x: x in self.query_span.get_tunable_optimizers(), conf))
        tmp_rules = list(filter(lambda x: x in self.query_span.get_tunable_rules(), conf))

        commands = list()
        if len(tmp_optimizers) > 0:
            commands.append(f'SET session {BAO_DISABLED_OPTIMIZERS} = ' + '\'{0}\''.format(','.join(tmp_optimizers)))
        if len(tmp_rules) > 0:
            commands.append(f'SET session {BAO_DISABLED_RULES} = ' + '\'{0}\''.format(','.join(tmp_rules)))
        return commands
