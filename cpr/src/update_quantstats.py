import quantstats as qs
import pandas as pd

# rp = pd.read_csv('../notes/rolling_159915_profit.csv', index_col=0, parse_dates=True)
# rp['profit'] = rp['profit_percent_weighted_avg']
# qs.reports.html(rp['profit'], output='../notes/rolling_159915_profit_report.html', title='QuantStats Report')

rp = pd.read_csv('../data/sig_worth/roll_worth_daily.csv', index_col=0, parse_dates=True)
rp['profit'] = rp['net_1_daily'].diff().fillna(rp['net_1_daily'])
print(rp)
qs.reports.html(rp['profit'], output='../notes/roll_worth_daily_report.html', title='QuantStats Report')
