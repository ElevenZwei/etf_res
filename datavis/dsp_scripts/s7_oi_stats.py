"""
这个脚本的作用是对 s5 计算出的 OI Intersect 数据计算统计信息。
它的统计信息的需求是对曲线的聚合和升降情况进行定量描述，
从而这些描述可以用在量化交易的 signal 设计中。
计算出的数据首先保存在 csv 文件中，然后通过 plotly 绘制出来，
人工检验这些统计信息是否符合预期。

"""

from dataclasses import dataclass, field

import click
import pandas as pd
import numpy as np
from scipy import stats

from dsp_config import DATA_DIR

def calc_spearman(df: pd.DataFrame, cols: list[str], res_col: str):
    """
    计算 Spearman 系数。 
    cols 输入表示每一行中数据列的理想顺序。
    每一行中的理想顺序是 cols[0] 数字最小， cols[-1] 数字最大 。
    对于 df 中的每一行，计算其与 cols 的 Spearman 系数。
    """
    rho_list = []
    b = np.arange(len(cols))
    for idx, row in df.iterrows():
        a = np.array(row[cols])
        rho, pval = stats.spearmanr(a, b)
        if rho < -0.99:
            rho = -1
        if rho > 0.99:
            rho = 1
        rho_list.append(rho)
    df[res_col] = rho_list
    return df

def calc_stdev(df: pd.DataFrame, cols: list[str], res_col: str):
    """
    计算标准差。
    """
    stdev_list = []
    for idx, row in df.iterrows():
        a = np.array(row[cols])
        stdev = np.std(a)
        stdev_list.append(stdev)
    df[res_col] = stdev_list
    return df

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

def calc_stats(df: pd.DataFrame):
    """
    1. 通过 columns 提取信息
    2. 把现在的 columns 分成几组计算
    """
    col_infos = extract_column_info(df)
    ts_list = np.unique([x.ts for x in col_infos])
    sigma_list = np.unique([x.sigma for x in col_infos])
    print(ts_list, sigma_list)
    for ts in ts_list:
        ts_cols = [x.name for x in col_infos if x.ts == ts]
        df = calc_spearman(df, ts_cols, f'oi_cp_spearman_ts_{ts}')
        df = calc_stdev(df, ts_cols, f'oi_cp_stdev_ts_{ts}')
    for sigma in sigma_list:
        sigma_cols = [x.name for x in col_infos if x.sigma == sigma]
        df = calc_spearman(df, sigma_cols, f'oi_cp_spearman_sigma_{sigma}')
        df = calc_stdev(df, sigma_cols, f'oi_cp_stdev_sigma_{sigma}')
    return df

def calc_stats_csv(spot: str, suffix: str):
    df = pd.read_csv(DATA_DIR / 'dsp_conv' / f'merged_{spot}_{suffix}.csv')
    df = calc_stats(df)
    df.to_csv(DATA_DIR / 'dsp_conv' / f'stats_{spot}_{suffix}.csv', index=False)

@click.command()
@click.option('-s', '--spot', type=str, required=True, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, required=True, help="csv file name suffix.")
def click_main(spot: str, suffix: str):
    calc_stats_csv(spot, suffix)

if __name__ == '__main__':
    click_main()

