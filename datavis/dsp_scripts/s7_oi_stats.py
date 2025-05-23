"""
这个脚本的作用是对 s5 计算出的 OI Intersect 数据计算统计信息。
它的统计信息的需求是对曲线的聚合和升降情况进行定量描述，
从而这些描述可以用在量化交易的 pos 设计中。
计算出的数据首先保存在 csv 文件中，然后通过 plotly 绘制出来，
人工检验这些统计信息是否符合预期。

"""

from dataclasses import dataclass, field
from multiprocessing import Pool

import click
import pandas as pd
import numpy as np
from scipy import stats

from dsp_config import DATA_DIR, POOL_SIZE, gen_wide_suffix
from helpers import OpenCloseHelper
import s9_trade_signal as s9

def calc_spearman(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """
    计算 Spearman 系数。 
    cols 输入表示每一行中数据列的理想顺序。
    每一行中的理想顺序是 cols[0] 数字最小， cols[-1] 数字最大 。
    对于 df 中的每一行，计算其与 cols 的 Spearman 系数。
    """
    b = np.arange(len(cols))

    def row_spearman(row):
        a = row[cols].to_numpy()
        if np.all(a == a[0]):
            return 0
        rho, _ = stats.spearmanr(a, b)
        if rho < -0.99:
            return -1
        if rho > 0.99:
            return 1
        return rho

    return df.apply(row_spearman, axis=1)

def calc_stdev(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    return df[cols].std(axis=1, ddof=0)  # ddof=0 表示与 np.std 默认行为一致

@dataclass(frozen=True)
class ColumnInfo:
    name: str = None
    ts: int = None
    sigma: float = None

def extract_column_info(df: pd.DataFrame):
    cols = [x for x in df.columns if x.startswith('oi_cp_')]
    col_infos = []
    for col in cols:
        ts, sigma = col.split('_')[2:]
        col_infos.append(ColumnInfo(name=col, ts=int(ts), sigma=float(sigma)))
    return col_infos

def calc_prop_stats(df: pd.DataFrame, col_infos: list[ColumnInfo], prop: str, value: float) -> pd.DataFrame:
    ts_cols = [x.name for x in col_infos if getattr(x, prop) == value]
    df_clip = df[ts_cols]
    spearman = calc_spearman(df_clip, ts_cols)
    stdev = calc_stdev(df_clip, ts_cols)
    dirstd = spearman * stdev
    return pd.DataFrame({
        f'oi_cp_spearman_{prop}_{value}': spearman,
        f'oi_cp_stdev_{prop}_{value}': stdev,
        f'oi_cp_dirstd_{prop}_{value}': dirstd,
    })

def calc_stats(df: pd.DataFrame):
    """
    1. 通过 columns 提取信息
    2. 把现在的 columns 分成几组计算
    """
    df['dt'] = pd.to_datetime(df['dt'])
    col_infos = extract_column_info(df)
    ts_set = {x.ts for x in col_infos}
    sigma_set = {x.sigma for x in col_infos}
    # print(f'calc stats begin for {len(ts_set)} ts and {len(sigma_set)} sigma')
    with Pool(POOL_SIZE) as pool:
        df_res = pool.starmap(calc_prop_stats, [
                *[(df, col_infos, 'ts', ts) for ts in ts_set],
                *[(df, col_infos, 'sigma', sigma) for sigma in sigma_set]
        ])
        df = pd.concat([df, *df_res], axis=1)
    # print(f'calc stats done for {len(ts_set)} ts and {len(sigma_set)} sigma')
    return df

def calc_long_short_pos(df: pd.DataFrame, wide: bool):
    """
    计算 long short pos
    """
    ts_long_open = 400
    ts_long_close = 100
    ts_short_open = -400
    ts_short_close = -100
    ts_helper = OpenCloseHelper(ts_long_open, ts_long_close, ts_short_open, ts_short_close)
    sigma_long_open = 220
    sigma_long_close = 10
    sigma_short_open = -220
    sigma_short_close = -10
    sigma_helper = OpenCloseHelper(sigma_long_open, sigma_long_close, sigma_short_open, sigma_short_close)
    ts_pos = []
    sigma_pos = []
    spot = str(df['spotcode'].iloc[0])
    ts_col = f'oi_cp_dirstd_ts_600'
    sigma_col = f'oi_cp_dirstd_sigma_{s9.get_sigma_width(spot, wide=wide)}'
    for idx, row in df.iterrows():
        if (row['dt'].hour == 9
                or row['dt'].hour == 10 and row['dt'].minute < 10
                or row['dt'].hour == 14 and row['dt'].minute > 47
                or row['dt'].hour == 15):
            ts_pos.append(0)
            sigma_pos.append(0)
            continue
        ts_pos.append(ts_helper.next(row[ts_col]))
        sigma_pos.append(sigma_helper.next(row[sigma_col]))
    df['ts_pos'] = ts_pos
    df['sigma_pos'] = sigma_pos
    return df

#  Ts 的参数现在还是一个重点调整对象。
#  除非是那种长牛趋势的日子，看起来现在的平仓信号只会太晚不会太早。
#  所以需要加入其他的平仓规则，例如说盘整就止盈，例如 PCP 止盈。
#  两个指标分开计算盈利然后加权也是一种做法。
#  或者平仓的时候两个指标分开平仓。

def calc_stats_csv(spot: str, suffix: str, wide: bool, show_pos: bool = True):
    suffix += gen_wide_suffix(wide)
    df = pd.read_csv(DATA_DIR / 'dsp_conv' / f'merged_{spot}_{suffix}.csv')
    df = calc_stats(df)
    if show_pos:
        df = calc_long_short_pos(df, wide=wide)
    df.to_csv(DATA_DIR / 'dsp_conv' / f'stats_{spot}_{suffix}.csv', index=False)

@click.command()
@click.option('-s', '--spot', type=str, required=True, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, required=True, help="csv file name suffix.")
@click.option('--wide', type=bool, default=False, help="wide or not.")
@click.option('--show_pos', type=bool, default=True, help="add pos or not.")
def click_main(spot: str, suffix: str, wide: bool, show_pos: bool = True):
    calc_stats_csv(spot, suffix, wide=wide, show_pos=show_pos)

if __name__ == '__main__':
    click_main()

