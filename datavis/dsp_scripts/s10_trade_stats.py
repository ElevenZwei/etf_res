"""
这个脚本的作用是统计 s9 生成的交易信号在多日内的表现情况。
需要几个函数。
一个用来统计当日交易的次数和盈亏情况，需要 Cooldown 参数做一个选择。
一个用来收集多日的统计结果。
"""

from collections import defaultdict
import datetime
import glob
import os

import click
import pandas as pd
import numpy as np

from dsp_config import DATA_DIR, gen_wide_suffix
import re

def calc_pos_price_maxmin(df: pd.DataFrame, buysell_signal_col: str):
    """计算每一个仓位状态里的最大最小值。"""
    price_df = df.groupby('pos_state_id')['spot_price'].agg(['max', 'min'])
    price_df.columns = ['spot_price_max', 'spot_price_min']

    """
    过滤交易点位需要区分先亏后赚还是先赚后亏。
    或者从先验后验的角度上说 Peak to Peak 的 Max Min 顺序不同的情况应该分开画成两张图。
    或者说各种事件到底可以给我们怎样的预期锁定的效果？在我的所有度量里面，如何根据一个一个呈现的信息确定现在在哪个区块里面？

    我们是否可以做一个 Peak to Peak 的序列分布统计？这个事情结合 OI 指标是否可以呈现一定程度上的规律？
    Peak to Peak 的序列的关键是找到哪些点是区域性的极值，值得被记录的极值，我可以用半小时尺度来刻画。

    PCP Series, P2P Series, Interval Series 这些都是一种尝试锁定分区的方法。
    """
    # 正向持仓的时候的最大回撤
    df['pos_state_spot_drawdown'] = df['spot_price'] - df.groupby('pos_state_id')['spot_price'].cummax() 
    drawdown_df = df.groupby('pos_state_id')['pos_state_spot_drawdown'].agg(['min'])
    drawdown_df.columns=['spot_price_max_drawdown']
    # 反向持仓的时候的最大回撤
    df['pos_state_spot_drawup'] = df['spot_price'] - df.groupby('pos_state_id')['spot_price'].cummin()
    drawup_df = df.groupby('pos_state_id')['pos_state_spot_drawup'].agg(['max'])
    drawup_df.columns=['spot_price_max_drawup']

    price_df = pd.concat([price_df, drawdown_df, drawup_df], axis=1)
    # print(price_df)
    return price_df

def intraday_timediff(A: datetime.datetime, B: datetime.datetime):
    if pd.isnull(A) or pd.isnull(B):
        return None
    res = A - B
    # skip 11:30 to 13:00
    if A.hour > 12 and B.hour < 12:
        res -= datetime.timedelta(hours=1, minutes=30)
    # print(A, B, res)
    return res

def calc_daily_stats(df: pd.DataFrame, buysell_signal_col: str, trades_per_day: int):
    df = df.sort_values(['dt'])
    df['pos_state_id'] = df[buysell_signal_col].ne(0).cumsum()
    price_df = calc_pos_price_maxmin(df, buysell_signal_col)
    # 假设它要到下个价格才能触发，防止买入上一个价格的东西。
    df['trade_price'] = df['spot_price'].shift(-1)
    df = df[df[buysell_signal_col] != 0].copy()
    if len(df) % 2 != 0:
        print('Error: the number of signals is not even, date:', df.iloc[0]['dt'])
        df = df.iloc[:-1]

    # 对于只有一层仓位的交易信号，可以用 shift 找到开平对应的数据行。
    df['close_dt'] = df['dt'].shift(-1)
    df['open_dt'] = df['dt']
    # df['close_spot_price'] = df['spot_price'].shift(-1)
    # df['open_spot_price'] = df['spot_price']
    df['close_spot_price'] = df['trade_price'].shift(-1)
    df['open_spot_price'] = df['trade_price']
    df['close_signal'] = df[buysell_signal_col].shift(-1)
    df['open_signal'] = df[buysell_signal_col]
    
    # keep only odd number lines.
    df = df[::2]
    df = df.join(price_df, on='pos_state_id')
    df['long_short'] = np.where(df['open_signal'] > 0, 1, -1)
    df['pnl'] = (df['close_spot_price'] - df['open_spot_price']) * df['long_short']
    df['hold_time'] = df.apply(lambda row: intraday_timediff(row['close_dt'], row['open_dt']), axis=1)
    df['close_dt_prev'] = df['close_dt'].shift(1)
    reopen_diff = df.apply(lambda row: intraday_timediff(row['open_dt'], row['close_dt_prev']), axis=1)
    df['pnl_max'] = np.where(df['long_short'] == 1, df['spot_price_max'] - df['open_spot_price'], df['open_spot_price'] - df['spot_price_min'])
    df['pnl_min'] = -1 * np.where(df['long_short'] == 1, df['open_spot_price'] - df['spot_price_min'], df['spot_price_max'] - df['open_spot_price'])
    # peak to peak loss and profit
    df['pnl_p2p_loss'] = np.where(df['long_short'] == 1, df['spot_price_max_drawdown'], -1 * df['spot_price_max_drawup'])
    df['pnl_p2p_profit'] = np.where(df['long_short'] == 1, df['spot_price_max_drawup'], -1 * df['spot_price_max_drawdown'])
    df = df[['open_dt', 'close_dt', 'long_short',
            'pnl', 'pnl_max', 'pnl_min',
            'pnl_p2p_profit', 'pnl_p2p_loss',
            'open_spot_price', 'close_spot_price',
            'spot_price_max', 'spot_price_min',
            'hold_time',
            ]]
    if not pd.isnull(reopen_diff).all():
        df['intraday_reopen_diff'] = reopen_diff
    df = df[:trades_per_day]
    return df

def filp_signal(df: pd.DataFrame, signal_col: str):
    """
    翻转信号，或者说翻转有持仓和没有持仓的状态，统计没有信号的时候的盈亏变化情况
    这个翻转要注意持仓间隔等等的问题。
    感觉还是单独做一个让 ETF 无波动的策略更容易调优。
    """
    pass

def calc_stats_one_day(df: pd.DataFrame, trades_per_day: int):
    signal_cols = [x for x in df.columns if x.endswith('_signal')]
    res = {}
    for col in signal_cols:
        res[col.replace('_signal', '')] = calc_daily_stats(df, col, trades_per_day)
    return res

def calc_stats_days(dfs: list[pd.DataFrame], trades_per_day: int):
    trades_dict = defaultdict(lambda: [])
    for df in dfs:
        trades = calc_stats_one_day(df, trades_per_day)
        for key in trades:
            trades_dict[key].append(trades[key])
    for key in trades_dict:
        # print(trades_dict[key])
        # exit(1)
        df_arr = [x for x in trades_dict[key] if len(x) != 0]
        df = pd.concat(df_arr)
        df['pnl_acc'] = df['pnl'].cumsum()
        df['hold_time_acc'] = df['hold_time'].cumsum()
        # move acc_pnl into position 2
        cols = df.columns.tolist()
        cols.insert(2, cols.pop(cols.index('pnl_acc')))
        cols.insert(3, cols.pop(cols.index('hold_time_acc')))
        df = df[cols]
        trades_dict[key] = df
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
    save_suffix = f"{bg_date.strftime('%Y%m%d')}_{ed_date.strftime('%Y%m%d')}{gen_wide_suffix(wide)}"
    for key in trades_dict:
        trades_dict[key].to_csv(DATA_DIR / 'dsp_stats'
                / f'{spot}_{key}_trades_{save_suffix}.csv', index=False,
                float_format='%.3f')
    return trades_dict

def calc_stats_all_csvs(
        spot: str, trades_per_day:int, wide:bool,
        bg_date: datetime.date, ed_date: datetime.date):
    dfs = []
    fs = glob.glob(f'{DATA_DIR}/dsp_conv/signal_{spot}_*_s5{gen_wide_suffix(wide)}.csv')

    # remove duplicated days with different expiry date
    date_pattern = re.compile(r'_exp(\d{8})_date(\d{8})_')
    unique_dates = {}
    for filepath in fs:
        match = date_pattern.search(filepath)
        if match:
            exp_str = match.group(1)
            date_str = match.group(2)
            dt = datetime.datetime.strptime(date_str, '%Y%m%d').date()
            if bg_date is not None and dt < bg_date:
                continue
            if ed_date is not None and dt > ed_date:
                continue
            if date_str not in unique_dates or exp_str < unique_dates[date_str]:
                unique_dates[date_str] = exp_str
    keep_fs = []
    for filepath in fs:
        match = date_pattern.search(filepath)
        if match:
            exp_str = match.group(1)
            date_str = match.group(2)
            if date_str in unique_dates and unique_dates[date_str] == exp_str:
                keep_fs.append(filepath)

    dfs = [pd.read_csv(filepath) for filepath in keep_fs]
    for x in dfs:
        x['dt'] = pd.to_datetime(x['dt'])
    trades_dict = calc_stats_days(dfs, trades_per_day)
    save_suffix = f'all{gen_wide_suffix(wide)}'
    for key in trades_dict:
        trades_dict[key].to_csv(DATA_DIR / 'dsp_stats' 
                / f'{spot}_{key}_trades_{save_suffix}.csv', index=False,
                float_format='%.3f')
    return trades_dict

def main(spot: str, exp_date: datetime.date,
        bg_date: datetime.date, ed_date: datetime.date, trades_per_day: int,
        wide: bool):
    calc_stats_csv(spot, exp_date, bg_date, ed_date, trades_per_day, wide)

def stat_batch(spot: str, trades_per_day: int, wide: bool):
    main(spot,
            bg_date=datetime.datetime(2024, 12, 1),
            ed_date=datetime.datetime(2024, 12, 25),
            exp_date=datetime.datetime(2024, 12, 25),
            trades_per_day=trades_per_day, wide=wide)
    main(spot,
            bg_date=datetime.datetime(2024, 12, 26),
            ed_date=datetime.datetime(2025, 1, 22),
            exp_date=datetime.datetime(2025, 1, 22),
            trades_per_day=trades_per_day, wide=wide)
    main(spot,
            bg_date=datetime.datetime(2025, 1, 23),
            ed_date=datetime.datetime(2025, 2, 26),
            exp_date=datetime.datetime(2025, 2, 26),
            trades_per_day=trades_per_day, wide=wide)
    main(spot,
            bg_date=datetime.datetime(2025, 2, 27),
            ed_date=datetime.datetime(2025, 3, 26),
            exp_date=datetime.datetime(2025, 3, 26),
            trades_per_day=trades_per_day, wide=wide)


@click.command()
@click.option('-s', '--spot', type=str, help="spot code: 159915 510050")
@click.option('-x', '--exp_date', type=str, help="expiry date.")
@click.option('-b', '--bg_date', type=str, help="begin date.")
@click.option('-e', '--ed_date', type=str, help="end date.")
@click.option('-t', '--trades_per_day', type=int, default=2, help="trades per day.")
@click.option('--wide', is_flag=True, type=bool, default=False, help="use wide data.")
@click.option('-a', '--stat-all', is_flag=True, type=bool, default=False, help="write into _all.csv file.")
def click_main(spot: str, exp_date: str,
        bg_date: str, ed_date: str, trades_per_day: int,
        wide: bool, stat_all: bool):
    if bg_date is not None:
        bg_date = datetime.datetime.strptime(bg_date, '%Y%m%d').date()
    if ed_date is not None:
        ed_date = datetime.datetime.strptime(ed_date, '%Y%m%d').date()
    if stat_all:
        calc_stats_all_csvs(spot, trades_per_day,
                wide=wide, bg_date=bg_date, ed_date=ed_date)
        # stat_batch(spot, trades_per_day, wide=wide)
    else:
        exp_date = datetime.datetime.strptime(exp_date, '%Y%m%d').date()
        main(spot, exp_date, bg_date, ed_date, trades_per_day, wide=wide)

if __name__ == '__main__':
    click_main()
