"""
做某一个时间宽度均值下，不同行权价宽度的 OI 曲面，我叫做卷轴曲面。
Open Interest Surface Calculation
Simplified to the following steps:
1. Use a filter on time axis.
2. Intepolate on strike axis.
3. Use different gaussian sigmas alongside the spot price.

直接读取 strike_oi_smooth 文件里面的 oi_cp_gau_ts 这一列, 作为时间均值的曲面。
"""

import pandas as pd

from dsp_config import DATA_DIR, get_spot_config

def pivot_df(df: pd.DataFrame):
    return df.pivot(index='strike', columns='dt', values='oi_cp_gau_ts')

def read_csv(spot: str, date: str):
    conf = get_spot_config(spot)
    time_width = conf.oi_ts_gaussian_sigmas[1]
    strike_width = conf.oi_strike_gaussian_sigmas[1]
    return pd.read_csv(f'strike_oi_smooth_{spot}_{date}_{time_width}_{strike_width}.csv')

