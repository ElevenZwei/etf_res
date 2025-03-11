"""
做某一个时间宽度均值下，不同行权价宽度的 OI 曲面，我叫做卷轴曲面。
Open Interest Surface Calculation
Simplified to the following steps:
1. Use a filter on time axis.
2. Intepolate on strike axis.
3. Use different gaussian sigmas alongside the spot price.
"""

import click
import pandas as pd
import numpy as np
import scipy.signal as ssig
import scipy.interpolate as sitp

from s1_dsp import remove_dup_cut, smooth_time_axis, smooth_spot_df, interpolate_strike_2, downsample_time, calc_window
from dsp_config import DATA_DIR, get_spot_config, gen_wide_suffix

DSP_SEC = 60

def read_file(spot: str, suffix: str, wide: bool):
    df = pd.read_csv(f'{DATA_DIR}/dsp_input/strike_oi_diff_{spot}_{suffix}.csv')
    df['spotcode'] = df['spotcode'].astype(str)
    df['dt'] = pd.to_datetime(df['dt'])
    # 在很多天的连续记录里面不能使用裁剪，只能计算 wide=True 的规律。
    df = remove_dup_cut(df, wide=wide)
    return df

def smooth_column_time_grid(
        opt_df: pd.DataFrame, col_name: str,
        dsp_sec: int, ts_sigma_sec: int):
    """对于某一列的数据，进行时间轴的平滑处理"""
    grid_1d = smooth_time_axis(opt_df, col_name, dsp_sec, ts_sigma_sec)
    grid_1d = interpolate_strike_2(grid_1d)
    return grid_1d

def sliding_window_with_padding(df, winsize):
    """让每一列的元素变成左右 N 列元素的数组"""
    result = []
    pad_size = (winsize - winsize % 2) // 2
    # 这里先转置 DataFrame，然后对每一列进行填充
    df_t = df.transpose()
    for colname in df_t.columns:
        # 对原先的每一行左右进行填充
        col = df_t[colname]
        padded_row = np.pad(col.values, (pad_size, pad_size), mode='edge')
        # 滑动窗口的方式获取原先 N 列对应的数组
        result.append([padded_row[i:i+winsize] for i in range(len(col))])
    return pd.DataFrame(result, columns=df.columns, index=df.index)

def sliding_melt(df: pd.DataFrame, winsize: int, col_name: str):
    df = sliding_window_with_padding(df, winsize)
    df = df.reset_index()
    df = df.melt(id_vars='dt', var_name='strike', value_name=col_name)
    df = df.set_index('dt')
    return df

def select_cols_with_index(row_idx, col_name, win_size, df1, df2):
    col_id = int(df1.loc[row_idx, col_name])
    return df2.loc[row_idx, col_id:col_id+win_size-1].values

def window_select(df, col_name, grid, winsize):
    pad_size = (winsize - winsize % 2) // 2
    grid_t = grid.transpose()
    result = []
    for grid_t_colname in grid_t.columns:
        col = grid_t[grid_t_colname]
        padded_row = np.pad(col.values, (pad_size, pad_size), mode='edge')
        result.append(padded_row)
    grid_pad = pd.DataFrame(result, index=grid.index)
    # print(grid_pad)
    select_data = [select_cols_with_index(idx, col_name, winsize, df, grid_pad) for idx in df.index]
    select_df = pd.DataFrame(select_data, index=df.index)
    # print(select_df)
    select_df[col_name] = select_df.apply(lambda row: row.tolist(), axis=1)
    select_df['price'] = df['price']
    select_df = select_df[['price', col_name]]
    # print(select_df)
    return select_df

def spot_intersect(spot_df: pd.DataFrame, oi_df: pd.DataFrame):
    spot_df = spot_df.reset_index()
    spot_df['dt'] = pd.to_datetime(spot_df['dt'])
    spot_df['spot_price'] = pd.to_numeric(spot_df['spot_price'])
    spot_df = spot_df.rename(columns={'spot_price': 'price'}).sort_values(['price', 'dt'])
    spot_df = spot_df[['dt', 'price']]

    oi_df = oi_df.reset_index()
    oi_df['dt'] = pd.to_datetime(oi_df['dt'])
    oi_df['strike'] = pd.to_numeric(oi_df['strike'])
    oi_df = oi_df.rename(columns={'strike': 'price'}).sort_values(['price', 'dt'])

    merged_df = pd.merge_asof(spot_df, oi_df, on='price', by='dt', direction='nearest')
    merged_df = merged_df.set_index('dt').sort_index()
    return merged_df

# 我这里设计一个每一列的数值就是列的编号的 Pivot Grid，然后用这个 Grid 做 spot intersect
# 再用这个编号去 df 里面切片对应的窗口，得到窗口之后再算点积
def strike_pivot_id_grid(strike_grid: pd.DataFrame):
    """对于一个 strike grid，生成一个编号的 pivot grid"""
    pivot_grid = pd.DataFrame(
            np.tile(np.arange(len(strike_grid.columns)), (len(strike_grid), 1)),
            columns=strike_grid.columns, index=strike_grid.index)
    return pivot_grid

def gaussian_dot_column(df: pd.DataFrame, col_name: str, winsize: int, sigma: int):
    """对于某一列的数据，进行高斯处理"""
    gau = ssig.windows.gaussian(winsize, sigma)
    gau = gau / gau.sum()
    # print(df)
    # print(f'gau shape={gau.shape}, col shape={df[col_name].iloc[0].shape}, winsize={winsize}, sigma={sigma}')
    df[col_name] = df[col_name].map(lambda x: np.dot(x, gau))
    df[col_name] = df[col_name].map(lambda x: np.nan if np.isnan(x).any() else x)
    return df

def sigmoid_dot_column(df: pd.DataFrame, col_name: str, winsize: int):
    """ 从 1/(1+Exp(-x)) 这个函数的 -4 到 4 范围里面取 winsize 个点，然后做点积 """
    sigmoid = 1 / (1 + np.exp(-np.linspace(-4, 4, winsize)))
    sigmoid = sigmoid / sigmoid.sum()
    df[col_name] = df[col_name].map(lambda x: np.dot(x, sigmoid))
    df[col_name] = df[col_name].map(lambda x: np.nan if np.isnan(x).any() else x)
    return df

def melt_intersect_dot(spot_df: pd.DataFrame, oi_df: pd.DataFrame, col_name: str,
        dsp_sec: int, ts_sigma: int, strike_sigma: float):
    grid_1d = smooth_column_time_grid(oi_df, col_name, dsp_sec, ts_sigma)
    # grid_1d.to_csv(f'{DATA_DIR}/tmp/grid_1d_{col_name}_{ts_sigma}_{strike_sigma}.csv')
    strikes = grid_1d.columns
    # print(strikes)
    win_size, sigma, med = calc_window(strikes, strike_sigma, 3.5)
    # 这一步防止过度 padding 这是 s5 和 s1 相比一开始遗漏的一步
    win_size = min(len(strikes), win_size)
    melt_df = sliding_melt(grid_1d, win_size, col_name)
    intersect_df = spot_intersect(spot_df, melt_df)
    # print("intersect_df:")
    # print(intersect_df)
    # intersect_df.to_csv(f'{DATA_DIR}/tmp/intersect_{col_name}_{ts_sigma}_{strike_sigma}.csv')
    # print(intersect_df)
    # print(win_size, sigma, med)
    intersect_df = gaussian_dot_column(intersect_df, col_name, win_size, sigma)
    return intersect_df

def melt_intersect_dot_2(spot_df: pd.DataFrame, oi_df: pd.DataFrame, col_name: str,
        dsp_sec: int, ts_sigma: int, strike_sigma: float):
    grid_1d = smooth_column_time_grid(oi_df, col_name, dsp_sec, ts_sigma)
    index_grid = strike_pivot_id_grid(grid_1d)
    # print(index_grid)
    index_melt = index_grid.reset_index().melt(id_vars='dt', var_name='strike', value_name=col_name).set_index('dt')
    intersect_df = spot_intersect(spot_df, index_melt)
    # print(intersect_df)
    strikes = grid_1d.columns
    win_size, sigma, med = calc_window(strikes, strike_sigma, 3.5)
    win_size = min(len(strikes), win_size)
    # print("win_size", win_size)
    select_df = window_select(intersect_df, col_name, grid_1d, win_size)
    select_df = gaussian_dot_column(select_df, col_name, win_size, sigma)
    # select_df = sigmoid_dot_column(select_df, col_name, win_size)
    return select_df

def cp_dot(spot_df: pd.DataFrame, oi_df: pd.DataFrame,
        dsp_sec: int, ts_sigma: int, strike_sigma: float,
        only_cp: bool):
    print(f'processing ts={ts_sigma}, strike={strike_sigma}')
    oi_df['oi_diff_cp'] = oi_df['oi_diff_c'] - oi_df['oi_diff_p']

    if only_cp:
        oi_diff_cp = melt_intersect_dot_2(spot_df, oi_df, 'oi_diff_cp', dsp_sec, ts_sigma, strike_sigma)
        cp = pd.concat([oi_diff_cp['oi_diff_cp']], axis=1)
        cp = cp.rename(columns={'oi_diff_cp': f'oi_cp_{ts_sigma}_{strike_sigma}'})
    else:
        oi_diff_c = melt_intersect_dot_2(spot_df, oi_df, 'oi_diff_c', dsp_sec, ts_sigma, strike_sigma)
        oi_diff_p = melt_intersect_dot_2(spot_df, oi_df, 'oi_diff_p', dsp_sec, ts_sigma, strike_sigma)
        cp = pd.concat([oi_diff_c['oi_diff_c'], oi_diff_p['oi_diff_p']], axis=1)
        cp['oi_diff_cp'] = cp['oi_diff_c'] - cp['oi_diff_p']
        cp = cp.rename(columns={
                'oi_diff_c': f'oi_c_{ts_sigma}_{strike_sigma}',
                'oi_diff_p': f'oi_p_{ts_sigma}_{strike_sigma}',
                'oi_diff_cp': f'oi_cp_{ts_sigma}_{strike_sigma}',
            })
    # print(cp)
    return cp

def cp_batch(spot_df: pd.DataFrame, oi_df: pd.DataFrame, dsp_sec: int,
        ts_sigma_list: list[int], strike_sigma_list: list[float],
        only_cp: bool):
    cp_list = []
    for ts_sigma in ts_sigma_list:
        for strike_sigma in strike_sigma_list:
            cp = cp_dot(spot_df, oi_df, dsp_sec, ts_sigma, strike_sigma, only_cp=only_cp)
            cp_list.append(cp)
    merged = pd.concat([spot_df, *cp_list], axis=1)
    return merged

def batch_rename(batch_df: pd.DataFrame):
    columns = batch_df.columns
    cols_map = {}
    for col in columns:
        if col.startswith('oi_'):
            cols_map[col] = col.split('_')[-1]
    batch_df = batch_df.rename(columns=cols_map)[cols_map.values()]
    return batch_df

def interpolate_melt(batch_df: pd.DataFrame, col_prefix: str):
    cols = [x for x in batch_df.columns if x.startswith(col_prefix)]
    res = interpolate_strike_2(batch_rename(batch_df[cols]))
    res = res.reset_index()
    res = res.melt(id_vars='dt', var_name='sigma', value_name=col_prefix + 'mean')
    res = res.set_index(['dt', 'sigma'])
    # print(res)
    return res

def calc_intersect(spot: str, suffix: str, wide: bool):
    df = read_file(spot, suffix, wide)
    spot_config = get_spot_config(spot)
    spot_df = smooth_spot_df(df, DSP_SEC, spot_config.oi_ts_gaussian_sigmas)
    cp_df = cp_batch(spot_df, df, DSP_SEC,
            spot_config.oi_ts_gaussian_sigmas,
            spot_config.get_strike_sigmas(wide),
            only_cp=True)
    cp_df.to_csv(f'{DATA_DIR}/dsp_conv/merged_{spot}_{suffix}_s5{gen_wide_suffix(wide)}.csv')

def calc_surface(spot: str, suffix: str):
    df = read_file(spot, suffix, wide=True)
    spot_config = get_spot_config(spot)
    spot_df = smooth_spot_df(df, DSP_SEC, spot_config.oi_ts_gaussian_sigmas)
    spot_df.to_csv(f'{DATA_DIR}/dsp_conv/spot_{spot}_{suffix}.csv')
    sigma_min = min(spot_config.get_strike_sigmas(wide=False))
    sigma_max = max(spot_config.get_strike_sigmas(wide=True)) * 1.4
    cp_df = cp_batch(spot_df, df, DSP_SEC,
            spot_config.oi_ts_gaussian_sigmas[1:2],
            np.around(np.arange(sigma_min, sigma_max, 0.05), decimals=2),
            only_cp=False)
    c_mean_df = interpolate_melt(cp_df, 'oi_c_')
    p_mean_df = interpolate_melt(cp_df, 'oi_p_')
    cp_mean_df = interpolate_melt(cp_df, 'oi_cp_')
    merged_df = pd.concat([c_mean_df, p_mean_df, cp_mean_df], axis=1)
    merged_df.to_csv(f'{DATA_DIR}/dsp_conv/oi_surface_{spot}_{suffix}.csv')

@click.command()
@click.option('-s', '--spot', type=str, required=True, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, help="csv file name suffix.")
@click.option('-w', '--wide', is_flag=True, default=False, help="Use wide strike sigma.")
def click_main(spot: str, suffix: str, wide: bool):
    calc_surface(spot, suffix)
    calc_intersect(spot, suffix, wide=wide)

if __name__ == '__main__':
    click_main()
    # calc_surface('159915', 'exp20250122_date20250108')
    # calc_intersect('159915', 'exp20250122_date20250108')


