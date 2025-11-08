# Input signal [-1, 1] and ETF price series, output net worth series

import pandas as pd
import numpy as np
from datetime import datetime
from typing import List

from config import DATA_DIR

def cut_df(df: pd.DataFrame, dt_from: datetime, dt_to: datetime):
    if df.index.name == 'dt':
        df = df.reset_index()
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


def merge_position(pos: pd.DataFrame, etf: pd.DataFrame, twap_count: int):
    pos = pos.set_index('dt')
    pos = pos[['position']]
    # some position files use NaN for no position
    pos['position'] = pos['position'].fillna(0)
    etf = etf.set_index('dt')
    etf = etf.rename(columns={ 'openp': 'spot_open', 'closep': 'spot_close' })
    etf = etf[['spot_open', 'spot_close']]
    etf = calc_spot_twap_trade_price(etf, cnt=twap_count)
    # print(etf.head())
    df = pos.join(etf, how='outer')
    return df


def calc_spot_twap_trade_price(etf: pd.DataFrame, cnt: int):
    price = etf['spot_close'].copy()
    for i in range(1, cnt):
        price += etf['spot_close'].shift(-i).ffill()
    price = price / cnt
    etf['spot_twap_trade_price'] = price
    print(etf.head(10))
    return etf


def cut_merge(pos: pd.DataFrame, etf: pd.DataFrame, dt_from: datetime, dt_to: datetime):
    pos = cut_df(pos, dt_from, dt_to)
    etf = cut_df(etf, dt_from, dt_to)
    return merge_position(pos, etf, 1)


def calc_intraday_profit(merged: pd.DataFrame):
    df = merged.copy()
    df['date'] = df.index.date
    df['spot_prev_trade_price'] = df['spot_twap_trade_price'].shift(1)
    df['spot_diff'] = df['spot_twap_trade_price'] - df['spot_prev_trade_price']
    df['spot_ratio'] = df['spot_twap_trade_price'] / df['spot_prev_trade_price'] - 1
    # shift to avoid lookahead bias
    df['position_actual'] = df['position'].shift(1)
    df['position_actual'] = df['position_actual'].ffill().fillna(0)
    # method 1 - fixed unit
    df['tick_1_diff'] = df['position_actual'] * df['spot_diff']
    df['net_1_intraday_diff'] = df.groupby('date')['tick_1_diff'].cumsum().fillna(0)
    # method 2 - compound investment
    df['tick_2_logret'] = np.log(df['position_actual'] * df['spot_ratio'] + 1)
    # sum(logret) over (partition by date order by dt)
    df['net_2_intraday_logret'] = df.groupby('date')['tick_2_logret'].cumsum().fillna(0)
    df['net_2_intraday'] = np.exp(df['net_2_intraday_logret']) - 1
    return df


def calc_worth(merged: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    df_intra = calc_intraday_profit(merged)
    df_daily = df_intra.groupby('date').agg({
        'spot_open': 'first',
        'net_1_intraday_diff': 'last',
        'net_2_intraday': 'last',
    })
    # method 1 - fixed unit
    df_daily['net_1_intraday'] = df_daily['net_1_intraday_diff'] / df_daily['spot_open']
    df_daily['net_1_daily'] = df_daily['net_1_intraday'].cumsum()
    df_daily['net_1_prev_daily'] = df_daily['net_1_daily'].shift(1).fillna(0)
    # method 2 - compound investment
    df_daily['net_2_daily'] = (df_daily['net_2_intraday'] + 1).cumprod() - 1
    # method 3 - intraday compound, daily cumulative
    df_daily['net_3_daily'] = df_daily['net_2_intraday'].cumsum()
    df_daily['net_3_prev_daily'] = df_daily['net_3_daily'].shift(1).fillna(0)

    # join back to intraday df
    df_daily_clip = df_daily[['spot_open', 'net_1_prev_daily', 'net_3_prev_daily']]
    df_daily_clip = df_daily_clip.rename(columns={'spot_open': 'spot_open_daily'})
    df_intra = df_intra.join(df_daily_clip, on='date', how='left')
    df_intra['net_1_intraday'] = df_intra['net_1_intraday_diff'] / df_intra['spot_open']
    df_intra['net_1_total'] = df_intra['net_1_prev_daily'] + df_intra['net_1_intraday']
    df_intra['net_2_total'] = np.exp(df_intra['tick_2_logret'].cumsum().ffill().fillna(0)) - 1
    df_intra['net_3_total'] = df_intra['net_3_prev_daily'] + df_intra['net_2_intraday']

    df_daily_final = df_daily[['spot_open', 'net_1_daily', 'net_2_daily', 'net_3_daily']]

    return df_intra, df_daily_final


def signal_worth(pos: pd.DataFrame, etf: pd.DataFrame,
                dt_from: datetime, dt_to: datetime):
    """
    Calculate net worth from position signal and etf price series.
    dt_from and dt_to are inclusive.
    """
    m = cut_merge(pos, etf, dt_from, dt_to)
    intra, daily = calc_worth(m)
    return intra, daily 


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
        merged = merge_position(df, etf)
        _, daily = calc_worth(merged)
        daily = daily[['net_1_daily', 'net_2_daily', 'net_3_daily']]
        daily = daily.rename(columns={
            'net_1_daily': f'net_1_{col}',
            'net_2_daily': f'net_2_{col}',
            'net_3_daily': f'net_3_{col}',
        })
        dfs.append(daily)
    df = pd.concat(dfs, axis=1)
    return df


def prepare_df(df: pd.DataFrame, dt_from: datetime, dt_to: datetime) -> pd.DataFrame:
    df = cut_df(df, dt_from, dt_to)
    df = df.set_index('dt')
    df = df[['position']]
    df['position'] = df['position'].fillna(0)
    return df


def main():
    # dt_from = datetime(2025, 1, 13)
    dt_from = datetime(2025, 10, 1)
    # dt_to = datetime(2025, 9, 30, 23, 59)
    dt_to = datetime(2025, 10, 31, 23, 59)
    etf1 = pd.read_csv(DATA_DIR / 'fact' / 'spot_159915_2025_dsp.csv')
    # p1 = pd.read_csv(DATA_DIR / 'signal' / 'pos_399006.csv')
    p2 = prepare_df(pd.read_csv(DATA_DIR / 'signal' / 'roll_159915_1.csv'), dt_from, dt_to)
    # p3 = prepare_df(pd.read_csv(DATA_DIR / 'signal' / 'roll_159915_2.csv'), dt_from, dt_to)
    intra, daily = signal_worth(p2, etf1, dt_from, dt_to)
    
    # df_signal = p2.join(p3, lsuffix='_1', rsuffix='_2', how='outer')
    # for col in df_signal.columns:
    #     df_signal[col] = df_signal[col].ffill().fillna(0)
    # m1 = signal_worth_mimo(df_signal.reset_index(), list(df_signal.columns),
    #                        etf1, dt_from, dt_to)

    intra.to_csv(DATA_DIR / 'sig_worth' / 'roll_worth_intraday.csv', index=True)
    daily.to_csv(DATA_DIR / 'sig_worth' / 'roll_worth_daily.csv', index=True)
    print(daily)
    return daily


# import matplotlib.pyplot as plt
if __name__ == '__main__':
    m1 = main()
    # m1.plot()
    # plt.show()


