import quantstats as qs
import pandas as pd

rp = pd.read_csv('../notes/rolling_159915_profit.csv', index_col=0, parse_dates=True)
rp['profit'] = rp['profit_percent_avg']
qs.reports.html(rp['profit'], output='../notes/rolling_159915_profit_report.html', title='QuantStats Report')
