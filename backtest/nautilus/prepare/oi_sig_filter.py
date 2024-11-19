"""
OI Sig 除去很多天连续一样的信号的时候。

"""

import pandas as pd

df = pd.read_csv('input/oi_signal_159915_raw.csv')
# df = df.loc[df['diff_act'] != df['prev_diff_act']]

df['dtdate'] = pd.to_datetime(df['dtdate']) 
df['dtdate_diff'] = df['dtdate'] - df['dtdate'].shift(1)
print(df['dtdate_diff'].sort_values())

df = df[['dt', 'code', 'mark_spot', 'diff_act']]
df = df.rename(columns={
    'mark_spot': 'price',
    'diff_act': 'action',
})
df.to_csv('input/oi_signal_159915_act_full.csv', index=False)