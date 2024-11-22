"""
对于连续数值的信号做一些初步的分析。
目前先做一个切片。
"""

import pandas as pd
from backtest.config import DATA_DIR

bgdt = pd.to_datetime('2024-01-01')
eddt = pd.to_datetime('2024-02-01')

df = pd.read_csv('input/sig_159915_digit.csv')
df.columns = ['dt', 'sigval']
df.loc[:, 'dt'] = pd.to_datetime(df['dt'])
df = df.loc[(df['dt'] > bgdt) & (df['dt'] < eddt)]
df.to_csv('input/sig_159915_digit_2024_clip.csv', index=False)
