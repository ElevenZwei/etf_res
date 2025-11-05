# Combine stock and cpr signals into new signal.

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from config import DATA_DIR
from sig_worth import cut_df, signal_worth_mimo

df_399 = pd.read_csv(DATA_DIR / 'signal' / 'pos_399006.csv')
df_159 = pd.read_csv(DATA_DIR / 'signal' / 'roll_159915_1.csv')
dt_from = pd.to_datetime('2025-01-13')
dt_to = pd.to_datetime('2025-09-19 23:59')

def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    df = cut_df(df, dt_from, dt_to)
    df = df.set_index('dt')
    df = df[['position']]
    df['position'] = df['position'].fillna(0)
    return df

df_159 = prepare_df(df_159)
df_399 = prepare_df(df_399)
df_signal = df_159.join(df_399, lsuffix='_159', rsuffix='_399', how='outer')
df_signal['position_159'] = df_signal['position_159'].ffill().fillna(0)
df_signal['position_diff'] = df_signal['position_159'] - df_signal['position_399']
df_signal['position_diff_abs'] = df_signal['position_diff'].abs()
df_signal['position_avg'] = (df_signal['position_159'] + df_signal['position_399']) / 2
# print(df_signal)

print(df_signal['position_diff'].describe())
print(df_signal['position_diff_abs'].describe())
df_signal.to_csv(DATA_DIR / 'signal' / 'combined' / 'pos_combined.csv', index=True)

# df_signal['position_diff'].cumsum().plot()
# df_signal.plot.hist(column=['position_diff_abs'], bins=20)
# plt.show()


def clamp(x, lower, upper):
    return max(lower, min(x, upper))

def diff_position_1a(r):
    if r['position_diff_abs'] > 0.7:
        return (r['position_399'] * 0.7 + r['position_159'] * 0.3)
    elif r['position_diff_abs'] < 0.5:
        return clamp(r['position_avg'] * 1.6, -1, 1)
    else:
        return r['position_avg']

def diff_position_1b(r):
    if r['position_diff_abs'] > 0.7:
        return (r['position_399'] * 0.7 + r['position_159'] * 0.3)
    elif r['position_diff_abs'] < 0.3:
        return clamp(r['position_avg'] * 1.4, -1, 1)
    else:
        return r['position_159']

def diff_position_1r(r):
    diff = r['position_diff_abs']
    if diff > 1.4:
        return 0
    if diff > 0.7:
        return (r['position_399'] * 0.3 + r['position_159'] * 0.7)
    elif diff < 0.3:
        return clamp(r['position_avg'] * 1.4, -1, 1)
    else:
        return r['position_avg']

def diff_position_2(r):
    if r['position_diff_abs'] > 0.8:
        return (r['position_399'] * 0.7 + r['position_159'] * 0.3)
    elif r['position_diff_abs'] < 0.4:
        return clamp(r['position_avg'] * 1.4, -1, 1)
    else:
        return r['position_avg']

def diff_position_2r(r):
    diff = r['position_diff_abs']
    if diff > 1.4:
        return 0
        # return (r['position_399'] * 0.8 + r['position_159'] * 0.2)
    if diff > 1:
        pct = (diff - 1) / 0.4
        p399 = (0.5 - 0.2) * pct + 0.2
        p159 = (0.5 - 0.8) * pct + 0.8
        return (r['position_399'] * p399 + r['position_159'] * p159)
        # return (r['position_399'] * 0.3 + r['position_159'] * 0.7)
    if diff > 0.7:
        pct = (diff - 0.7) / 0.3
        p399 = (0.2 - 0.5) * pct + 0.5
        p159 = (0.8 - 0.5) * pct + 0.5
        return clamp((r['position_399'] * p399 + r['position_159'] * p159), -1, 1)
    if diff > 0.5:
        return r['position_avg']
    elif diff > 0.4:
        return clamp(r['position_avg'] * 1.2, -1, 1)
    else:
        return clamp(r['position_avg'] * 2, -1, 1)

def diff_position_2rs(r):
    pos = diff_position_2r(r)
    if pos > 0.15:
        return 1
    if pos < -0.15:
        return -1
    return 0

def diff_position_3(r):
    if r['position_diff_abs'] > 0.5:
        return (r['position_399'] * 0.7 + r['position_159'] * 0.3)
    elif r['position_diff_abs'] < 0.2:
        return clamp(r['position_avg'] * 1.6, -1, 1)
    else:
        return r['position_avg']

def diff_position_3r(r):
    if r['position_diff_abs'] > 0.5:
        return (r['position_399'] * 0.3 + r['position_159'] * 0.7)
    elif r['position_diff_abs'] < 0.2:
        return clamp(r['position_avg'] * 1.6, -1, 1)
    else:
        return r['position_avg']

def mask_position_1(r):
    if r['position_diff_abs'] < 0.6:
        return r['position_159']
    else:
        return r['position_avg']

def sign_position_1(r, col):
    avg = r[col]
    if avg < -0.2:
        return np.maximum(avg * 3, -1)
    if avg > 0.2:
        return np.minimum(avg * 3, 1)
    return 0

def sign_position_1x(r, col):
    avg = r[col]
    if avg < -0.1:
        return np.maximum(avg * 7, -1)
    if avg > 0.1:
        return np.minimum(avg * 7, 1)
    return 0


def sign_position_1y(r, col):
    avg = r[col]
    if avg < -0.4:
        return np.maximum(avg * 2, -1)
    if avg > 0.4:
        return np.minimum(avg * 2, 1)
    if avg < -0.2:
        return avg
    if avg > 0.2:
        return avg
    return 0


def sign_position_2(r, col):
    avg = r[col]
    diff = r['position_diff_abs']
    if diff > 1.4:
        return 0
    if avg < -0.2:
        return np.maximum(avg * 3, -1)
    if avg > 0.2:
        return np.minimum(avg * 3, 1)
    return 0

def sign_position_2x(r, col):
    avg = r[col]
    diff = r['position_diff_abs']
    if diff > 1.4:
        return 0
    if avg < -0.15:
        return -1
    if avg > 0.15:
        return 1
    return 0

def sign_position_2z(r, col):
    avg = r[col]
    diff = r['position_diff_abs']
    if diff > 1.4:
        return 0
    if avg < -0.15:
        return np.maximum(avg * 6, -1)
    if avg > 0.15:
        return np.minimum(avg * 6, 1)
    return 0


def combine_1():
    df1 = df_signal.copy()
    df1['position_cd1a'] = df1.apply(diff_position_1a, axis=1)
    df1['position_cd1b'] = df1.apply(diff_position_1b, axis=1)
    df1['position_cd1r'] = df1.apply(diff_position_1r, axis=1)
    df1['position_cd2'] = df1.apply(diff_position_2, axis=1)
    df1['position_cd2r'] = df1.apply(diff_position_2r, axis=1)
    df1['position_cd2rs'] = df1.apply(diff_position_2rs, axis=1)
    df1['position_cd3'] = df1.apply(diff_position_3, axis=1)
    df1['position_cd3r'] = df1.apply(diff_position_3r, axis=1)
    df1['position_cm1'] = df1.apply(mask_position_1, axis=1)

    df1['position_3a7b'] = df1['position_399'] * 0.7 + df1['position_159'] * 0.3
    df1['position_7a3b'] = df1['position_399'] * 0.3 + df1['position_159'] * 0.7

    df1['position_cs1'] = df1.apply(lambda r: sign_position_1(r, 'position_avg'), axis=1)
    df1['position_cs1x'] = df1.apply(lambda r: sign_position_1x(r, 'position_avg'), axis=1)
    df1['position_cs1y'] = df1.apply(lambda r: sign_position_1y(r, 'position_avg'), axis=1)
    df1['position_cs2'] = df1.apply(lambda r: sign_position_2(r, 'position_avg'), axis=1)
    df1['position_cs2z'] = df1.apply(lambda r: sign_position_2z(r, 'position_avg'), axis=1)
    df1['position_cs2x'] = df1.apply(lambda r: sign_position_2x(r, 'position_avg'), axis=1)

    # df1['position_cs159'] = df1.apply(lambda r: sign_position_1(r, 'position_159'), axis=1)
    df1['position_cs1x_159'] = df1.apply(lambda r: sign_position_1x(r, 'position_159'), axis=1)
    # df1['position_cs399'] = df1.apply(lambda r: sign_position_1(r, 'position_399'), axis=1)
    df1['position_cs1x_399'] = df1.apply(lambda r: sign_position_1x(r, 'position_399'), axis=1)
    df1['position_cs1x_3a7b'] = df1.apply(lambda r: sign_position_1x(r, 'position_3a7b'), axis=1)
    df1['position_cs1x_7a3b'] = df1.apply(lambda r: sign_position_1x(r, 'position_7a3b'), axis=1)
    df1['position_cs2z_3a7b'] = df1.apply(lambda r: sign_position_2z(r, 'position_3a7b'), axis=1)
    df1['position_cs2z_7a3b'] = df1.apply(lambda r: sign_position_2z(r, 'position_7a3b'), axis=1)
    df1['position_cs2x_3a7b'] = df1.apply(lambda r: sign_position_2x(r, 'position_3a7b'), axis=1)
    df1['position_cs2x_7a3b'] = df1.apply(lambda r: sign_position_2x(r, 'position_7a3b'), axis=1)

    return df1

df1 = combine_1().reset_index()
# df1['position_cumsum'] = df1['position'].cumsum()
# df1['position_159_cumsum'] = df1['position_159'].cumsum()
# df1['position_399_cumsum'] = df1['position_399'].cumsum()
# dfp = df1[['position_cumsum', 'position_159_cumsum', 'position_399_cumsum']]
# dfp.plot()

etf1 = pd.read_csv(DATA_DIR / 'fact' / 'spot_159915_2025_dsp.csv')
worth1 = signal_worth_mimo(df1,
       [
           *[col for col in df1.columns if col.startswith('position_c')],
           *['position_avg', 'position_159', 'position_399'],
       ],
       etf1, dt_from, dt_to)
worth1.to_csv(DATA_DIR / 'sig_worth' / 'combine1.csv')

net_1_cols = [col for col in worth1.columns if col.startswith('net_1_position')]
# worth1d = worth1.resample('1d').agg({ col: 'last' for col in net_1_cols }).dropna()
worth1d = worth1
worth1d.to_csv(DATA_DIR / 'sig_worth' / 'combine1d.csv')
# print(worth1d)

# 收益相关性比较
corr1 = worth1d['net_1_position_159'].corr(worth1d['net_1_position_399'])
corr1d = worth1d['net_1_position_159'].diff().corr(worth1d['net_1_position_399'].diff())
print('net corr: ', corr1)
print('net diff corr:', corr1d)

# 收益特征比较
means, stds = [], []
vol_sums = []
for col in net_1_cols:
    desc = worth1d[col].diff().describe()
    means.append(desc['mean'])
    stds.append(desc['std'])
    input_col = col.replace('net_1_position_', 'position_')
    vol_s = df1[input_col].diff().abs().sum()
    vol_sums.append(vol_s)
    print(desc)
net_1_char = pd.DataFrame({'mean': means, 'std': stds, 'vol_sum': vol_sums, 'name': net_1_cols, })
net_1_char = net_1_char.set_index('name')
net_1_char['return_per_year'] = net_1_char['mean'] * 252
net_1_char['ratio'] = net_1_char['mean'] / net_1_char['std'] * 252**0.5
net_1_char = net_1_char.sort_values(by='mean')

print(net_1_char)
net_1_char_159 = net_1_char.loc['net_1_position_159']
net_1_char_399 = net_1_char.loc['net_1_position_399']
net_1_char_ends = pd.concat([net_1_char_159.to_frame().T, net_1_char_399.to_frame().T])


fig, ax = plt.subplots(figsize=(5, 5), layout='constrained')
# ax.scatter(net_1_char['std'], net_1_char['mean'], label='combinations')
# ax.plot(net_1_char_ends['std'], net_1_char_ends['mean'], label='baseline')
# ax.set_xlabel('std')
# ax.set_ylabel('mean')
# ax.plot(worth1d.index, worth1d['net_1_position_cd1a'], label='cd1a')
# ax.plot(worth1d.index, worth1d['net_1_position_cd1b'], label='cd1b')
# ax.plot(worth1d.index, worth1d['net_1_position_cd1r'], label='cd1r')
ax.plot(worth1d.index, worth1d['net_1_position_cd2r'], label='cd2r')
ax.plot(worth1d.index, worth1d['net_1_position_cd2rs'], label='cd2rs')

# ax.plot(worth1d.index, worth1d['net_1_position_cs1'], label='cs1')
ax.plot(worth1d.index, worth1d['net_1_position_cs1x'], label='cs1x')
ax.plot(worth1d.index, worth1d['net_1_position_cs1y'], label='cs1y')
ax.plot(worth1d.index, worth1d['net_1_position_cs2'], label='cs2')
ax.plot(worth1d.index, worth1d['net_1_position_cs2z'], label='cs2')
ax.plot(worth1d.index, worth1d['net_1_position_cs2x'], label='cs2')

ax.plot(worth1d.index, worth1d['net_1_position_cs1x_3a7b'], label='cs1x_3a7b')
ax.plot(worth1d.index, worth1d['net_1_position_cs1x_7a3b'], label='cs1x_7a3b')
ax.plot(worth1d.index, worth1d['net_1_position_cs2x_3a7b'], label='cs2x_3a7b')
ax.plot(worth1d.index, worth1d['net_1_position_cs2x_7a3b'], label='cs2x_7a3b')
ax.plot(worth1d.index, worth1d['net_1_position_cs2z_3a7b'], label='cs2z_3a7b')
ax.plot(worth1d.index, worth1d['net_1_position_cs2z_7a3b'], label='cs2z_7a3b')
ax.plot(worth1d.index, worth1d['net_1_position_cs1x_159'], label='cs1x_159')
ax.plot(worth1d.index, worth1d['net_1_position_cs1x_399'], label='cs1x_399')
# # ax.plot(worth1d.index, worth1d['net_1_position_cm1'], label='cm1')
ax.plot(worth1d.index, worth1d['net_1_position_159'], label='159')
ax.plot(worth1d.index, worth1d['net_1_position_399'], label='399')
ax.plot(worth1d.index, worth1d['net_1_position_avg'], label='avg')

ax.legend()
plt.show()

# 这里可以做一些真正实验性的东西。
# 我们发现 cd2r 和 cd2rs 效果较好，且波动较小，可以考虑进一步优化这两个策略。
