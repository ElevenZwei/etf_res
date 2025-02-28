"""
这个脚本的作用是统计 s9 生成的交易信号在多日内的表现情况。
需要几个函数。
一个用来统计当日交易的次数和盈亏情况，需要 Cooldown 参数做一个选择。
一个用来收集多日的统计结果。
"""

from collections import defaultdict
import datetime
import os

import click
import pandas as pd
import numpy as np

from dsp_config import DATA_DIR, gen_wide_suffix

def calc_pos_price_maxmin(df: pd.DataFrame, buysell_signal_col: str):
    """计算每一个仓位状态里的最大最小值。"""
    price_df = df.groupby('pos_state_id')['spot_price'].agg(['max', 'min'])
    price_df.columns = ['spot_price_max', 'spot_price_min']
    return price_df

def calc_daily_stats(df: pd.DataFrame, buysell_signal_col: str, trades_per_day: int):
    df['pos_state_id'] = df[buysell_signal_col].ne(0).cumsum()
    price_df = calc_pos_price_maxmin(df, buysell_signal_col)
    df = df[df[buysell_signal_col] != 0].copy()
    if len(df) % 2 != 0:
        print('Error: the number of signals is not even, date:', df.iloc[0]['dt'])
        df = df.iloc[:-1]

    # 对于只有一层仓位的交易信号，可以用 shift 找到开平对应的数据行。
    df['close_dt'] = df['dt'].shift(-1)
    df['open_dt'] = df['dt']
    df['close_spot_price'] = df['spot_price'].shift(-1)
    df['open_spot_price'] = df['spot_price']
    df['close_signal'] = df[buysell_signal_col].shift(-1)
    df['open_signal'] = df[buysell_signal_col]
    
    # keep only odd number lines.
    df = df[::2]
    df = df.join(price_df, on='pos_state_id')
    df['long_short'] = np.where(df['open_signal'] > 0, 1, -1)
    df['pnl'] = (df['close_spot_price'] - df['open_spot_price']) * df['long_short']
    df['hold_time'] = df['close_dt'] - df['open_dt']
    df['intraday_reopen_diff'] = df['open_dt'] - df['close_dt'].shift(1)
    df['pnl_max'] = np.where(df['long_short'] == 1, df['spot_price_max'] - df['open_spot_price'], df['open_spot_price'] - df['spot_price_min'])
    df['pnl_min'] = -1 * np.where(df['long_short'] == 1, df['open_spot_price'] - df['spot_price_min'], df['spot_price_max'] - df['open_spot_price'])
    df = df[['open_dt', 'close_dt', 'hold_time',
            'intraday_reopen_diff',
            'open_spot_price', 'close_spot_price',
            'spot_price_max', 'spot_price_min',
            'long_short',
            'pnl_max', 'pnl_min', 'pnl']]
    df = df[:trades_per_day]
    return df

def calc_stats_one_day(df: pd.DataFrame, trades_per_day: int):
    ts_trades = calc_daily_stats(df, 'ts_signal', trades_per_day)
    sigma_trades = calc_daily_stats(df, 'sigma_signal', trades_per_day)
    ts_sigma_trades = calc_daily_stats(df, 'ts_sigma_signal', trades_per_day)
    toss_trades = calc_daily_stats(df, 'toss_signal', trades_per_day)
    tosr_trades = calc_daily_stats(df, 'tosr_signal', trades_per_day)
    totp_trades = calc_daily_stats(df, 'totp_signal', trades_per_day)
    return {
        'ts': ts_trades,
        'sigma': sigma_trades,
        'ts_sigma': ts_sigma_trades,
        'toss': toss_trades,
        'tosr': tosr_trades,
        'totp': totp_trades,
    }

def calc_stats_days(dfs: list[pd.DataFrame], trades_per_day: int):
    trades_dict = defaultdict(lambda: [])
    for df in dfs:
        trades = calc_stats_one_day(df, trades_per_day)
        for key in trades:
            trades_dict[key].append(trades[key])
    for key in trades_dict:
        # print(trades_dict[key])
        # exit(1)
        trades_dict[key] = pd.concat(trades_dict[key])
        trades_dict[key]['acc_pnl'] = trades_dict[key]['pnl'].cumsum()
    return trades_dict

def calc_stats_csv(spot: str, exp_date: datetime.date,
        bg_date: datetime.date, ed_date: datetime.date, trades_per_day: int,
        wide: bool):
    dfs = []
    exp_str = exp_date.strftime('%Y%m%d')
    for date in pd.date_range(bg_date, ed_date):
        if date.weekday() >= 5:
            continue
        date_str = date.strftime('%Y%m%d')
        filepath = (DATA_DIR / 'dsp_conv'
                / f'signal_{spot}_exp{exp_str}_date{date_str}_s5{gen_wide_suffix(wide)}.csv')
        if not os.path.exists(filepath):
            print(f"cannot find {filepath}, skip.")
            continue
        df = pd.read_csv(filepath)
        df['dt'] = pd.to_datetime(df['dt'])
        dfs.append(df)
    trades_dict = calc_stats_days(dfs, trades_per_day)
    save_suffix = f'{bg_date.strftime('%Y%m%d')}_{ed_date.strftime('%Y%m%d')}{gen_wide_suffix(wide)}'
    for key in trades_dict:
        trades_dict[key].to_csv(DATA_DIR / 'dsp_stats'
                / f'{spot}_{key}_trades_{save_suffix}.csv', index=False,
                float_format='%.3f')

def main(spot: str, exp_date: datetime.date,
        bg_date: datetime.date, ed_date: datetime.date, trades_per_day: int,
        wide: bool):
    calc_stats_csv(spot, exp_date, bg_date, ed_date, trades_per_day, wide)

@click.command()
@click.option('-s', '--spot', type=str, help="spot code: 159915 510050")
@click.option('-x', '--exp_date', type=str, help="expiry date.")
@click.option('-b', '--bg_date', type=str, help="begin date.")
@click.option('-e', '--ed_date', type=str, help="end date.")
@click.option('-t', '--trades_per_day', type=int, default=2, help="trades per day.")
@click.option('--wide', type=bool, default=False, help="use wide data.")
def click_main(spot: str, exp_date: str,
        bg_date: str, ed_date: str, trades_per_day: int,
        wide: bool):
    exp_date = datetime.datetime.strptime(exp_date, '%Y%m%d').date()
    bg_date = datetime.datetime.strptime(bg_date, '%Y%m%d').date()
    ed_date = datetime.datetime.strptime(ed_date, '%Y%m%d').date()
    main(spot, exp_date, bg_date, ed_date, trades_per_day, wide=wide)

if __name__ == '__main__':
    click_main()
