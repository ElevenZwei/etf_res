# Combine stock and cpr signals into new signal.

import pandas as pd
import matplotlib.pyplot as plt

from config import DATA_DIR
from sig_worth import cut_df, signal_worth_mimo

df_399 = pd.read_csv(DATA_DIR / 'signal' / 'pos_399006.csv')
df_159 = pd.read_csv(DATA_DIR / 'signal' / 'roll_159915.csv')
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
print(df_signal)

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

def sign_position_1(r):
    avg = r['position_avg']
    if avg < -0.2:
        return -1
    if avg > 0.2:
        return 1
    return 0

def sign_position_2(r):
    avg = r['position_avg']
    diff = r['position_diff_abs']
    if diff > 1.4:
        return 0
    if avg < -0.15:
        return -1
    if avg > 0.15:
        return 1
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
    df1['position_cs1'] = df1.apply(sign_position_1, axis=1)
    df1['position_cs2'] = df1.apply(sign_position_2, axis=1)
    return df1

df1 = combine_1().reset_index()
# df1['position_cumsum'] = df1['position'].cumsum()
# df1['position_159_cumsum'] = df1['position_159'].cumsum()
# df1['position_399_cumsum'] = df1['position_399'].cumsum()
# dfp = df1[['position_cumsum', 'position_159_cumsum', 'position_399_cumsum']]
# dfp.plot()

etf1 = pd.read_csv(DATA_DIR / 'fact' / 'oi_159915_full.csv')
worth1 = signal_worth_mimo(df1,
       [
           *[col for col in df1.columns if col.startswith('position_c')],
           *['position_avg', 'position_159', 'position_399'],
       ],
       etf1, dt_from, dt_to)
worth1.to_csv(DATA_DIR / 'sig_worth' / 'combine1.csv')

net_2_cols = [col for col in worth1.columns if col.startswith('net_2_position')]
worth1d = worth1.resample('1d').agg({ col: 'last' for col in net_2_cols }).dropna()
worth1d.to_csv(DATA_DIR / 'sig_worth' / 'combine1d.csv')
print(worth1d)

# 收益相关性比较
corr1 = worth1d['net_2_position_159'].corr(worth1d['net_2_position_399'])
corr1d = worth1d['net_2_position_159'].diff().corr(worth1d['net_2_position_399'].diff())
print('net corr: ', corr1)
print('net diff corr:', corr1d)

# 收益特征比较
means, stds = [], []
for col in net_2_cols:
    desc = worth1d[col].diff().describe()
    means.append(desc['mean'])
    stds.append(desc['std'])
    print(desc)
net_2_char = pd.DataFrame({'mean': means, 'std': stds, 'name': net_2_cols})
net_2_char['ratio'] = net_2_char['mean'] / net_2_char['std']
net_2_char = net_2_char.sort_values(by='mean')
print(net_2_char)
net_2_char_ends = pd.concat([net_2_char.head(1), net_2_char.tail(1)])

fig, ax = plt.subplots(figsize=(5, 5), layout='constrained')
# ax.scatter(net_2_char['std'], net_2_char['mean'], label='combinations')
# ax.plot(net_2_char_ends['std'], net_2_char_ends['mean'], label='baseline')
# ax.set_xlabel('std')
# ax.set_ylabel('mean')
# ax.plot(worth1d.index, worth1d['net_2_position_cd1a'], label='cd1a')
# ax.plot(worth1d.index, worth1d['net_2_position_cd1b'], label='cd1b')
# ax.plot(worth1d.index, worth1d['net_2_position_cd1r'], label='cd1r')
ax.plot(worth1d.index, worth1d['net_2_position_cd2r'], label='cd2r')
ax.plot(worth1d.index, worth1d['net_2_position_cd2rs'], label='cd2rs')
ax.plot(worth1d.index, worth1d['net_2_position_cs1'], label='sign1')
ax.plot(worth1d.index, worth1d['net_2_position_cs2'], label='sign2')
# ax.plot(worth1d.index, worth1d['net_2_position_cm1'], label='cm1')
ax.plot(worth1d.index, worth1d['net_2_position_159'], label='159')
ax.plot(worth1d.index, worth1d['net_2_position_399'], label='399')
ax.plot(worth1d.index, worth1d['net_2_position_avg'], label='avg')
ax.legend()
plt.show()
