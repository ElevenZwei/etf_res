"""
这里换一个不同的维度 plot 几种交易参数设置的优劣。
我们这里尝试读取所有的 spot all csv 绘制 PNL 曲线。
先写汇总到一天之内并且换算成 percent 收益的算法。
"""

import bisect
import click
import glob
import pandas as pd
import numpy as np

from dsp_config import DATA_DIR, gen_wide_suffix

def daily_rollup(df: pd.DataFrame):
    arg_desc = df['arg_desc'].iloc[0]
    df['date'] = pd.to_datetime(df['open_dt']).dt.date
    df['hold_time'] = pd.to_timedelta(df['hold_time'])
    df['pnl_p'] = (df['close_spot_price'] / df['open_spot_price'] - 1) * df['long_short']
    df = df[['date', 'pnl', 'pnl_p', 'hold_time']]
    res = pd.DataFrame()
    res['cnt'] = df.groupby('date').size()
    res['cnt_acc'] = res['cnt'].cumsum()
    res['pnl'] = df.groupby('date')['pnl'].sum()
    res['pnl_acc'] = res['pnl'].cumsum()
    res['pnl_p'] = df.groupby('date')['pnl_p'].sum()
    res['pnl_p_acc'] = res['pnl_p'].cumsum()
    res['hold_time'] = df.groupby('date')['hold_time'].sum()
    res['hold_time_acc'] = res['hold_time'].cumsum()
    res['arg_desc'] = arg_desc
    # print(res)
    return res

def prefix_match(sorted_list, prefix):
    # return [item for item in sorted_list if item.startswith(prefix)]
    idx = bisect.bisect_left(sorted_list, prefix)
    matched = []
    while idx < len(sorted_list) and sorted_list[idx].startswith(prefix):
        matched.append(sorted_list[idx])
        idx += 1
    return matched

def sort_cols(cols: list[str]):
    # 这里我们按照一定的顺序来排序列
    cols.sort()
    order = ['pnl', 'pnl_acc', 'cnt', 'cnt_acc', 'pnl_p', 'pnl_p_acc', 'hold_time', 'hold_time_acc']
    order = [x + '@' for x in order]
    sorted_cols = []
    for prefix in order:
        matched = prefix_match(cols, prefix)
        sorted_cols.extend(matched)
    for col in cols:
        if col not in sorted_cols:
            sorted_cols.append(col)
    return sorted_cols

def merge_rollup_df(dfs: list[pd.DataFrame]):
    res = []
    for df in dfs:
        arg_desc = df['arg_desc'].iloc[0]
        df = df.drop(columns=['arg_desc'])
        df.columns = [f'{col}@{arg_desc}' for col in df.columns]
        res.append(df)
        # print(df)
    res = pd.concat(res, axis=1)
    res = res[sort_cols(list(res.columns))]
    res = res.sort_index()
    return res

def read_csv(spot: str, suffix: str):
    fs = glob.glob(f'{DATA_DIR}/dsp_stats/{spot}_*_trades_{suffix}.csv')
    res = []
    for fpath in fs:
        df = pd.read_csv(fpath)
        if len(df) == 0:
            continue
        res.append(df)
    return res

def main(spot: str, suffix: str):
    dfs = read_csv(spot, suffix)
    dfs = [daily_rollup(df) for df in dfs]
    res = merge_rollup_df(dfs)
    res.to_csv(f'{DATA_DIR}/dsp_stats/{spot}_compare_rollup_{suffix}.csv', float_format='%.3f')
    # print(res)
    return res

# main('510500', 'all')
# df = daily_rollup(pd.read_csv(
#         f'{DATA_DIR}/dsp_stats/510500_totp3_trades_all.csv'))
# merge_rollup_df([df])

@click.command()
@click.option('-s', '--spot', type=str, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, help="csv file name suffix.")
def click_main(spot: str, suffix: str):
    main(spot, suffix)


if __name__ == '__main__':
    click_main()
