"""This module implements a wrapper class for performance predictions from Bao, which is used for visualizations and plots."""


class PerformancePrediction:
    """Class to store the performance predictions from Bao for a certain query and all the generated QEPs for further evaluation"""

    def __init__(self, default_plan_runtime, selected_plan_runtime, best_plan_runtime, query_path, is_training_sample=False):
        self.default_plan_runtime = default_plan_runtime
        self.selected_plan_runtime = selected_plan_runtime  # chosen by Bao
        self.best_plan_runtime = best_plan_runtime  # the best configuration for this query
        self.query_path = query_path
        self.relative_improvement = (default_plan_runtime - selected_plan_runtime) / default_plan_runtime
        self.absolute_improvement = (default_plan_runtime - selected_plan_runtime)
        self.is_training_sample = is_training_sample
