"""Plot the distribution of rules, consider effective, alternative, and required rules"""
from matplotlib import pyplot as plt
from benchmark_plotter import style, texify, colors
import pandas as pd
import pandasql as pdsql
from storage import _db, read_sql_file

style.set_custom_style()
texify.latexify(3.39, 1.2)

fig, axes = plt.subplots(nrows=1, ncols=1, constrained_layout=True)

with _db() as conn:
    stmt = read_sql_file('evaluation/rule_per_query_distribution.sql')
    cursor = conn.execute(stmt)
    result = cursor.fetchall()
df = pd.DataFrame(result)
labels = ['effective', 'required', 'alternative']

# todo add the results from the EXPLAIN-based query span approximation as well
query_span = pd.read_csv('query_span_approximation.csv')

_avg = pdsql.sqldf('select avg(effective_rules) as er, avg(required_rules) as rr, avg(alternative_rules) as ar from df')
ymin = pdsql.sqldf('select min(effective_rules) as er, min(required_rules) as rr, min(alternative_rules) as ar from df')
ymax = pdsql.sqldf('select max(effective_rules) as er, max(required_rules) as rr, max(alternative_rules) as ar from df')
ymin = _avg.subtract(ymin)
ymax = ymax.subtract(_avg)
yerr = ymin.append(ymax)

axes.set_yticks([0, 10, 20, 30])
axes.set_ylabel(r'\#~rules')

axes.bar(x=labels, height=_avg.values[0], yerr=yerr.values, capsize=2, color=[colors.colors['blue'], colors.colors['red'], colors.colors['lightorange']])

plt.savefig('evaluation/figures/rule_distributions.pdf', bbox_inches='tight')
