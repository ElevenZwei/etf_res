"""
借用一下 dsp_scripts s0 的代码，
从 PG 数据库下载的 raw tick csv 里面读取 oi 数据。
然后计算 oi sum 输出。
"""

import os
import glob
import pandas as pd
import datetime
import click
import sys
from pathlib import Path

sys.path.append((Path(__file__).resolve().parent.parent / 'dsp_scripts').as_posix())
from s0_md_query import save_fpath_default, auto_dl_default

def dl_raw_if_missing(spot: str, dt: datetime.date):
    """
    如果没有下载过，下载数据。
    """
    fpath, _ = save_fpath_default(spot, 'raw', dt)
    if not os.path.exists(fpath):
        print(f"downloading {fpath}")
        auto_dl_default(spot, dt)
    return fpath

def pivot_sum(df: pd.DataFrame, col: str = 'oi'):
    """
    计算 oi 数据的总和。
    """
    df = df.pivot(index='dt', columns='strike', values=col)
    df = df.astype('int64').ffill().bfill()
    return df.sum(axis=1)

def calc_oi(df: pd.DataFrame):
    df = df.drop_duplicates(subset=['dt', 'tradecode'], keep='first')
    call_df = df[df['callput'] == 1]
    call_sum = pivot_sum(call_df, 'oi')
    put_df = df[df['callput'] == -1]
    put_sum = pivot_sum(put_df, 'oi')
    df2 = pd.DataFrame({
        'call_oi_sum': call_sum,
        'put_oi_sum': put_sum,
    })
    df2['pc'] = df2['put_oi_sum'] - df2['call_oi_sum']
    spot_price = df[['dt', 'spot_price']].drop_duplicates()
    spot_price = spot_price.set_index('dt')
    df2 = pd.merge(df2, spot_price, left_index=True, right_index=True, how='inner')
    return df2.reset_index()

def read_calc_oi(spot: str, dt: datetime.date):
    """
    计算 oi 数据。
    """
    fpath, _ = save_fpath_default(spot, 'raw', dt)
    df = pd.read_csv(fpath)
    # 计算 oi 数据
    df = calc_oi(df)
    print(df)
    # 保存结果
    fpath_oi, _ = save_fpath_default(spot, 'oi', dt)
    df.to_csv(fpath_oi, index=False)
    return fpath_oi

@click.command()
@click.option('-s', '--spot', type=str, required=True, help="spot code: 159915 510050")
@click.option('-b', '--begin', type=str, help="format is %Y%m%d")
@click.option('-e', '--end', type=str, help="format is %Y%m%d")
def click_main(spot: str, begin: str, end: str):
    """
    计算 oi 数据。
    """
    begin = datetime.datetime.strptime(begin, '%Y%m%d').date()
    end = datetime.datetime.strptime(end, '%Y%m%d').date()
    dt_list = pd.date_range(begin, end).to_list()
    for dt in dt_list:
        if dt.weekday() > 5:
            continue
        try:
            dl_raw_if_missing(spot, dt)
            read_calc_oi(spot, dt)
        except Exception as e:
            print(f"{dt} failed, error: {e}")
            # print error message into a log file
            with open('error_log.txt', 'a') as f:
                f.write(f'{dt} failed, error: {e}\n')

if __name__ == '__main__':
    click_main()
    pass