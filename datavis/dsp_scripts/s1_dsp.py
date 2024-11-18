# 对原始的高频 OI 值做一个滤波之后降低它的采样频率，
# 我应该分别对 Call 和 Put 操作
# 然后对 Strike 轴进行 cubic 插值
# 然后再对 Strike 轴做高斯卷积
# 输出一个新的 CSV，里面标注降低采样频率之后的滤波值，以及这个位置的卷积值。
# 输出的 OI 值控制在分钟级别
# 输出有很精细的 strike，每个 strike 需要下面这些数值
# oi_c_gau_ts, oi_p_gau_ts, oi_cp_gau_ts
# oi_c_gau_2d, oi_p_gau_2d, oi_cp_gau_2d

import pandas as pd
import numpy as np
import scipy.signal as ssig
import scipy.interpolate as sitp
import click

def left_gaussian(sig, wsize, sigma):
    wsize = min(wsize, len(sig))
    gau = ssig.windows.gaussian(wsize, sigma)
    gau[:wsize // 2] = 0
    gau /= np.sum(gau)
    # 在卷积边缘位置填充边缘数值
    # valid 模式下的输出长度是 N-K+1 如果 K 是单数，那么填充 K//2*2 刚好是 K-1 个长度，如果是双数就会多一个。
    sig = np.pad(sig, pad_width=wsize // 2, mode='edge')
    res = np.convolve(sig, gau, mode='valid')
    if wsize % 2 == 0:
        res = res[:-1]
    return res

def full_gaussian(sig, wsize, sigma):
    wsize = min(wsize, len(sig))
    gau = ssig.windows.gaussian(wsize, sigma)
    gau /= np.sum(gau)
    sig = np.pad(sig, pad_width=wsize // 2, mode='edge')
    res = np.convolve(sig, gau, mode='valid')
    if wsize % 2 == 0:
        res = res[:-1]
    return res

def gaussian_every_column(df: pd.DataFrame, wsize, sigma, use_left_gaussian):
    df = df.copy()
    for col in df.columns:
        se = df[col]
        if use_left_gaussian:
            df[col] = left_gaussian(se, wsize, sigma)
        else:
            df[col] = full_gaussian(se, wsize, sigma)
    return df

def downsample_time(df: pd.DataFrame, interval_sec: int):
    df = df.resample(f'{interval_sec}s').first()
    # 这里跳过没有开盘的时间
    df = df[~df.isna().all(axis=1)]
    return df

def interpolate_strike(pivot_df: pd.DataFrame):
    """ Input pivoted df with time index and strike columns. """
    na_lines = pivot_df[pivot_df.isna().any(axis=1)]
    if (na_lines.shape[0] > 0):
        # NaN 在插值的时候会有大幅传染
        print("interpolate input has nan lines:")
        print(na_lines)
        pivot_df.ffill(inplace=True)
    x_uni = pivot_df.columns.values   # strike type float64
    y_uni = pivot_df.index.values     # dt type datetime64
    y_uni = pivot_df.index.astype('int64') / 1e12 - 1.72947e6 # type float64
    # 这一步生成长度相同的 x, y, z 数组
    x_grid, y_grid = np.meshgrid(x_uni, y_uni)
    x_flat = x_grid.flatten()
    y_flat = y_grid.flatten()
    z_flat = pivot_df.values.flatten()

    x_hres = np.linspace(np.min(x_uni), np.max(x_uni), 200)
    x_hres_grid, y_hres_grid = np.meshgrid(x_hres, y_uni)
    z_hres_grid = sitp.griddata(
        (x_flat, y_flat), z_flat,
        (x_hres_grid, y_hres_grid),
        method='cubic'
    )
    res = pd.DataFrame(z_hres_grid, index=pivot_df.index, columns=x_hres)
    # print(pivot_df)
    # print(res)
    return res

def calc_window(series, sigma, multi):
    diff = series[1:] - series[:-1]
    diff_med = np.median(diff)
    res_sigma = int(sigma / diff_med)
    res_wsize = int(res_sigma * multi)
    return res_wsize, res_sigma, diff_med 

def smooth_column_2d_grid(df: pd.DataFrame, col_name: str,
                          dsp_sec: int, ts_sigma_sec, strike_sigma_price):
    df = df.sort_values(['dt', 'strike'])
    df = df.drop_duplicates()
    duplicate_lines_bitmap = df.groupby(['dt', 'strike']).transform('size') > 1
    dup_lines = df.loc[duplicate_lines_bitmap]
    if dup_lines.shape[0] > 0:
        print("df has dup lines:")
        print(dup_lines)
        df = df.set_index('dt')
        # 这个是分组之后组内重新采样，这个用 SQL 非常难刻画，不知道为什么 SQL 在数据变形方面并不好用。
        df = df.groupby('strike').resample('1s').first().drop(columns=['strike']).reset_index()
        df = df[df.notna().all(axis=1)]
        print("df after remove dup lines:")
        print(df)

    grid_1d = df.pivot(index='dt', columns='strike', values=col_name)
    # print(grid_1d)
    grid_1d.ffill(inplace=True)
    grid_1d.fillna(0, inplace=True)
    grid_1d.astype('Int64').to_csv(f'../tmp/grid_1d_input_{col_name}.csv')
    # print(grid_1d)
    # grid_1d = downsample_time(grid_1d, 60)
    se_ts = grid_1d.index.astype('int64') // 10**9
    ts_wsize, ts_sigma, ts_diff_med = calc_window(se_ts, ts_sigma_sec, 3.5)
    print(f"ts_diff_med={ts_diff_med}, ts_wsize={ts_wsize}, ts_sigma={ts_sigma}")

    grid_1d = gaussian_every_column(grid_1d, ts_wsize, ts_sigma, use_left_gaussian=True)
    grid_1d = downsample_time(grid_1d, dsp_sec)
    grid_1d = interpolate_strike(grid_1d)

    strike_wsize, strike_sigma, strike_med = calc_window(grid_1d.columns, strike_sigma_price, 2.5)
    print(f"strike_diff_med={strike_med}, strike_wsize={strike_wsize}, strike_sigma={strike_sigma}")
    grid_2d = grid_1d.transpose()
    grid_2d = gaussian_every_column(grid_2d, strike_wsize, strike_sigma, use_left_gaussian=False)
    grid_2d = grid_2d.transpose()
    return grid_1d, grid_2d

def smooth_column(df: pd.DataFrame, input_name: str, out1_name: str, out2_name: str,
                  dsp_sec: int, ts_sigma_sec, strike_sigma_price):
    oi_grid, oi_grid_2d = smooth_column_2d_grid(
            df, input_name, dsp_sec, ts_sigma_sec, strike_sigma_price)
    oi_1d = oi_grid.reset_index().melt(id_vars='dt', value_name=out1_name, var_name='strike')
    oi_2d = oi_grid_2d.reset_index().melt(id_vars='dt', value_name=out2_name, var_name='strike')
    return oi_1d.set_index(['dt', 'strike']), oi_2d.set_index(['dt', 'strike'])

def smooth_oi_csv(df: pd.DataFrame, dsp_sec, ts_sigma_sec, strike_sigma_price):
    oi_c_1d, oi_c_2d = smooth_column(
            df, 'oi_diff_c', 'oi_c_gau_ts', 'oi_c_gau_2d',
            dsp_sec, ts_sigma_sec, strike_sigma_price)
    oi_p_1d, oi_p_2d = smooth_column(
            df, 'oi_diff_p', 'oi_p_gau_ts', 'oi_p_gau_2d',
            dsp_sec, ts_sigma_sec, strike_sigma_price)
    df['oi_diff_cp'] = df['oi_diff_c'] - df['oi_diff_p']
    oi_cp_1d, oi_cp_2d = smooth_column(
            df, 'oi_diff_cp', 'oi_cp_gau_ts', 'oi_cp_gau_2d',
            dsp_sec, ts_sigma_sec, strike_sigma_price)
    df_res = pd.concat([oi_c_1d, oi_c_2d, oi_p_1d, oi_p_2d, oi_cp_1d, oi_cp_2d], axis=1)
    df_res['spotcode'] = df['spotcode'][0]
    df_res['expirydate'] = df['expirydate'][0]
    return df_res

def smooth_spot_df(df: pd.DataFrame, dsp_sec, ts_sigma_sec_list: list[int]):
    df = df[['dt', 'spotcode', 'spot_price']].drop_duplicates()
    df['spotcode'] = df['spotcode'].astype('str')
    se_ts = df['dt'].astype('int64').values // 10**9

    for ts_sigma_sec in ts_sigma_sec_list:
        ts_wsize, ts_sigma, ts_diff_med = calc_window(se_ts, ts_sigma_sec, 3.5)
        print(f"for ts_sigma_sec={ts_sigma_sec}, ts_diff_med={ts_diff_med}, ts_wsize={ts_wsize}, ts_sigma={ts_sigma}")
        df[f'spot_price_{ts_sigma_sec}'] = left_gaussian(df['spot_price'], ts_wsize, ts_sigma)

    df = df.set_index('dt')
    df = downsample_time(df, dsp_sec)
    return df

# 这个是为了绘图编写的 DSP 函数的配置方案。
def dsp_file_2_plot(spot: str, date: str):
    # 这个时间滤波窗口的大小根据做不同波段的因果验证可以有不同的调整。
    df = pd.read_csv(f'../dsp_input/strike_oi_diff_{spot}_{date}.csv')
    df['dt'] = pd.to_datetime(df['dt'])

    df_res = smooth_oi_csv(df, dsp_sec=120, ts_sigma_sec=1200, strike_sigma_price=0.3)
    df_res.to_csv(f'../dsp_plot/strike_oi_smooth_{spot}_{date}.csv')

    df_spot = smooth_spot_df(df, dsp_sec=120, ts_sigma_sec_list=[300])
    df_spot.to_csv(f'../dsp_plot/spot_{spot}_{date}.csv')


# 这个是为了计算不同长度的相关关系做的 dsp
def dsp_file_2_intersect(spot: str, suffix: str, ts_sigma_list: list[int], strike_sigma_list: list[float]):
    df = pd.read_csv(f'../dsp_input/strike_oi_diff_{spot}_{suffix}.csv')
    df['dt'] = pd.to_datetime(df['dt'])
    for ts_sigma in ts_sigma_list:
        for strike_sigma in strike_sigma_list:
            df_res = smooth_oi_csv(df, dsp_sec=60, ts_sigma_sec=ts_sigma, strike_sigma_price=strike_sigma)
            df_res.to_csv(f'../dsp_conv/strike_oi_smooth_{spot}_{suffix}_{ts_sigma}_{strike_sigma}.csv')
    df_spot = smooth_spot_df(df, dsp_sec=60, ts_sigma_sec_list=ts_sigma_list)
    df_spot.to_csv(f'../dsp_conv/spot_{spot}_{suffix}.csv')
    
def dsp_plot_tasks():
    # plot_dsp_file('159915', '20241017')
    # plot_dsp_file('159915', '20241018')
    # plot_dsp_file('159915', '20241021')
    # plot_dsp_file('159915', '20241023')
    # plot_dsp_file('159915', '20241101')
    # plot_dsp_file('159915', '20241104')
    dsp_file_2_plot('510050', '20241114_am')
    pass

ts_sigma_list = [120, 300, 600, 1200]
strike_sigma_list = [0.3, 0.4, 0.5, 0.6, 0.8]
def dsp_conv_tasks():
    # data_dsp_file('159915', '20241017m', ts_sigma_list, strike_sigma_list)
    # data_dsp_file('159915', '20241018', ts_sigma_list, strike_sigma_list)
    # data_dsp_file('159915', '20241021', ts_sigma_list, strike_sigma_list)
    # data_dsp_file('159915', '20241022', ts_sigma_list, strike_sigma_list)
    # data_dsp_file('159915', '20241023', ts_sigma_list, strike_sigma_list)
    # data_dsp_file('159915', '20241101', ts_sigma_list, strike_sigma_list)
    # data_dsp_file('159915', '20241104', ts_sigma_list, strike_sigma_list)
    # data_dsp_file('159915', '20241105', ts_sigma_list, strike_sigma_list)
    # data_dsp_file('159915', '20241106', ts_sigma_list, strike_sigma_list)
    # data_dsp_file('159915', '20241108', ts_sigma_list, strike_sigma_list)
    # data_dsp_file('159915', '20241112', ts_sigma_list, strike_sigma_list)
    dsp_file_2_intersect('510050', '20241114_am', ts_sigma_list, strike_sigma_list)
    pass

def main(spot: str, suffix: str):
    dsp_file_2_plot(spot, suffix)
    dsp_file_2_intersect(spot, suffix, ts_sigma_list, strike_sigma_list)

@click.command()
@click.option('-s', '--spot', type=str, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, help="csv file name suffix.")
def click_main(spot: str, suffix: str):
    main(spot, suffix)

if __name__ == '__main__':
    # dsp_plot_tasks()
    # dsp_conv_tasks()
    click_main()
    pass

