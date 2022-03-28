"""This module implements a json to graphviz converter"""
from presto_query_plan.operators import ENCODED_TYPES
import os
import numpy as np


class PlanPrinter:
    """The PlanPrinter converts a given query plan to the graphviz format"""

    def __init__(self, plan):
        self.plan = plan
        self.node_id = 0
        self.output = 'digraph plan {\n'

    def print_binarized_dot(self, filename):
        self._print_binarized_dot_rec(self.plan)
        self.output += '\n}'

        with open(filename + '.dot', 'w') as f:
            f.write(self.output)
        os.system('dot -Tpng {0}.dot -o {0}.png'.format(filename))

    def _print_binarized_dot_rec(self, node, parent_id=-1):
        self.node_id += 1
        if len(node) > 3:
            node_type = ENCODED_TYPES[np.argmax(node == 1)] if np.argmax(node == 1) < len(ENCODED_TYPES) else 'NULL'
            self.output += '\n\tnode_{0}[label=\"{1}:{2}\", shape=record]'.format(self.node_id, node_type, str(node))
            if parent_id > -1:
                self.output += 'node_{0} -> node_{1};'.format(parent_id, self.node_id)
        else:
            assert len(node) == 3
            n, l, r = node
            node_type = ENCODED_TYPES[np.argmax(n == 1)]
            self.output += '\n\tnode_{0}[label=\"{1}:{2}\", shape=record]'.format(self.node_id, node_type, str(n))
            if parent_id > -1:
                self.output += 'node_{0} -> node_{1};'.format(parent_id, self.node_id)
            n_id = self.node_id
            self._print_binarized_dot_rec(l, parent_id=n_id)
            self._print_binarized_dot_rec(r, parent_id=n_id)
