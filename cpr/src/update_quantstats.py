import quantstats as qs
import pandas as pd

# rp = pd.read_csv('../notes/rolling_159915_profit.csv', index_col=0, parse_dates=True)
# rp['profit'] = rp['profit_percent_weighted_avg']
# qs.reports.html(rp['profit'], output='../notes/rolling_159915_profit_report.html', title='QuantStats Report')

rp = pd.read_csv('../data/sig_worth/roll_worth_daily.csv', index_col=0, parse_dates=True)
rp['profit'] = (1 + rp['net_1_daily']).pct_change().fillna(rp['net_1_daily'])
rp['profit_accum'] = (1 + rp['profit']).cumprod() - 1
print(rp[['net_1_daily', 'profit_accum', 'profit']].tail())
qs.reports.html(rp['profit'], output='../notes/roll_worth_daily_report.html', title='QuantStats Report')
