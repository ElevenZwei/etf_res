# Input signal [-1, 1] and ETF price series, output net worth series

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List

from config import DATA_DIR

def cut_df(df: pd.DataFrame, dt_from: datetime, dt_to: datetime):
    df['dt'] = pd.to_datetime(df['dt'])
    # call tz_localize or tz_convert
    if df['dt'].dt.tz is None:
        df['dt'] = df['dt'].dt.tz_localize('Asia/Shanghai')
    else:
        df['dt'] = df['dt'].dt.tz_convert('Asia/Shanghai')
    dt_from = dt_from.replace(tzinfo=df['dt'].dt.tz)
    dt_to = dt_to.replace(tzinfo=df['dt'].dt.tz)
    df = df.loc[(df['dt'] >= dt_from) & (df['dt'] <= dt_to)]
    return df


def merge_position(pos: pd.DataFrame, etf: pd.DataFrame):
    pos = pos.set_index('dt')
    pos = pos[['position']]
    # some position files use NaN for no position
    pos['position'] = pos['position'].fillna(0)
    etf = etf.set_index('dt')
    etf = etf.resample('1min').agg({
            'spot_price': ['first', 'last'],
           }).dropna()
    etf.columns = etf.columns.droplevel(0)
    etf.columns = ['spot_open', 'spot_close']
    # print(etf.head())
    df = pos.join(etf, how='outer')
    return df


def cut_merge(pos: pd.DataFrame, etf: pd.DataFrame, dt_from: datetime, dt_to: datetime):
    pos = cut_df(pos, dt_from, dt_to)
    etf = cut_df(etf, dt_from, dt_to)
    return merge_position(pos, etf)


def calc_worth(merged: pd.DataFrame):
    df = merged.copy()
    df['spot_prev_close'] = df['spot_close'].shift(1)
    df['spot_diff'] = df['spot_close'] - df['spot_prev_close']
    df['spot_ratio'] = df['spot_close'] / df['spot_prev_close'] - 1
    # shift to avoid lookahead bias
    df['position_actual'] = df['position'].shift(1)
    df['position_actual'] = df['position_actual'].ffill().fillna(0)

    # method 1 fixed unit
    # method 2 compound investment
    df['worth_1_diff'] = df['position_actual'] * df['spot_diff']
    df['net_1_diff'] = df['worth_1_diff'].cumsum().ffill().fillna(0)
    df['net_1'] = df['net_1_diff'] / df['spot_close']
    df['worth_2_logret'] = np.log(df['position_actual'] * df['spot_ratio'] + 1)
    df['net_2_logret'] = df['worth_2_logret'].cumsum().ffill().fillna(0)
    df['net_2'] = np.exp(df['net_2_logret']) - 1

    return df


def signal_worth(pos: pd.DataFrame, etf: pd.DataFrame,
                dt_from: datetime, dt_to: datetime):
    """
    Calculate net worth from position signal and etf price series.
    dt_from and dt_to are inclusive.
    """
    m = cut_merge(pos, etf, dt_from, dt_to)
    m = calc_worth(m)
    return m


def signal_worth_mimo(pos: pd.DataFrame,
                      pos_columns: List[str],
                      etf: pd.DataFrame,
                      dt_from: datetime, dt_to: datetime):
    """input 'pos' should have a 'dt' column."""
    pos = cut_df(pos, dt_from, dt_to)
    etf = cut_df(etf, dt_from, dt_to)
    dfs = []
    for col in pos_columns:
        df = pos[['dt', col]].copy()
        df = df.rename(columns={col: 'position'})
        m = merge_position(df, etf)
        m = calc_worth(m)
        m = m[['net_1', 'net_2']]
        m = m.rename(columns={
            'net_1': f'net_1_{col}',
            'net_2': f'net_2_{col}',
        })
        dfs.append(m)
    df = pd.concat(dfs, axis=1)
    return df


def prepare_df(df: pd.DataFrame, dt_from: datetime, dt_to: datetime) -> pd.DataFrame:
    df = cut_df(df, dt_from, dt_to)
    df = df.set_index('dt')
    df = df[['position']]
    df['position'] = df['position'].fillna(0)
    return df


def main():
    dt_from = datetime(2025, 1, 13)
    dt_to = datetime(2025, 9, 30, 23, 59)
    etf1 = pd.read_csv(DATA_DIR / 'fact' / 'oi_159915_full.csv')
    # p1 = pd.read_csv(DATA_DIR / 'signal' / 'pos_399006.csv')
    p2 = prepare_df(pd.read_csv(DATA_DIR / 'signal' / 'roll_159915_1.csv'), dt_from, dt_to)
    p3 = prepare_df(pd.read_csv(DATA_DIR / 'signal' / 'roll_159915_2.csv'), dt_from, dt_to)
    df_signal = p2.join(p3, lsuffix='_1', rsuffix='_2', how='outer')
    for col in df_signal.columns:
        df_signal[col] = df_signal[col].ffill().fillna(0)
    # print(df_signal)
    m1 = signal_worth_mimo(df_signal.reset_index(), list(df_signal.columns),
                           etf1, dt_from, dt_to)
    m1.to_csv(DATA_DIR / 'sig_worth' / 'roll_worth.csv', index=True)
    print(m1)
    return m1


# import matplotlib.pyplot as plt
if __name__ == '__main__':
    m1 = main()
    # m1.plot()
    # plt.show()


