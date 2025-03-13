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

from dsp_config import DATA_DIR, get_spot_config, gen_wide_suffix

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
    df = df.loc[~df.isna().all(axis=1)]
    return df

def interpolate_strike_2(pivot_df: pd.DataFrame):
    na_lines = pivot_df[pivot_df.isna().any(axis=1)]
    if (na_lines.shape[0] > 0):
        # NaN 在插值的时候会有大幅传染
        print("interpolate input has nan lines:")
        print(na_lines)
        pivot_df.ffill(inplace=True)

    x_uni = pivot_df.columns.values.astype(np.float64)  # strike price array: type float64
    x_hres = np.linspace(np.min(x_uni), np.max(x_uni), 200)
    
    def cubic_spline(row):
        cs = sitp.CubicSpline(x_uni, row)
        return cs(x_hres)
    
    df = pd.DataFrame(np.vstack(pivot_df.apply(cubic_spline, axis=1)),
            index=pivot_df.index, columns=x_hres)
    # print(df)
    return df

def calc_window(series, sigma, multi):
    diff = series[1:] - series[:-1]
    diff_med = np.median(diff)
    res_sigma = int(sigma / diff_med)
    res_wsize = int(res_sigma * multi)
    return res_wsize, res_sigma, diff_med 

def remove_dup_lines(df: pd.DataFrame):
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
        df = df.loc[df.notna().all(axis=1)]
        print("df after remove dup lines:")
        print(df)
    return df

def smooth_time_axis(df: pd.DataFrame, col_name: str, dsp_sec: int, ts_sigma_sec: int):
    # 如果有不常交易的期权漏掉一段时间的数据通过 pivot 和 ffill 都能补齐。
    grid_1d = df.pivot(index='dt', columns='strike', values=col_name)
    grid_1d.ffill(inplace=True)
    grid_1d.fillna(0, inplace=True)
    grid_1d = downsample_time(grid_1d, dsp_sec)
    se_ts = grid_1d.index.astype('int64') // 10**9
    ts_wsize, ts_sigma, ts_diff_med = calc_window(se_ts, ts_sigma_sec, 3.5)
    # print(f"ts_sigma_sec={ts_sigma_sec}, dsp_sec={dsp_sec}, ts_diff_med={ts_diff_med}, ts_wsize={ts_wsize}, ts_sigma={ts_sigma}")
    grid_1d = gaussian_every_column(grid_1d, ts_wsize, ts_sigma, use_left_gaussian=True)
    return grid_1d

def smooth_column_2d_grid(df: pd.DataFrame, col_name: str,
                          dsp_sec: int, ts_sigma_sec, strike_sigma_price):
    grid_1d = smooth_time_axis(df, col_name, dsp_sec, ts_sigma_sec)
    grid_1d = interpolate_strike_2(grid_1d)

    strike_wsize, strike_sigma, strike_med = calc_window(grid_1d.columns, strike_sigma_price, 2.5)
    # print(f"strike_sigma_price={strike_sigma_price}, strike_diff_med={strike_med}, strike_wsize={strike_wsize}, strike_sigma={strike_sigma}")
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

# 排除赌狗以及我也不理解他们出于什么样的目的参与交易的人的影响
# 如果数据里面当日开盘的 ask < 0.0005 那么就把这个期权去掉。
# 我发现这个会增加很多的数据传输量，我可以想一个更简单的办法，
# 比如说只留下距离平值 20% 以内的。
def cut_off_degenerate_gambler(df: pd.DataFrame, keep_percent: float):
    spot = df.loc[:, 'spot_price'].iloc[0]
    strikes_se = df.loc[:, 'strike']
    strikes = strikes_se.unique()
    # print("before cut: ", strikes)
    strikes = [x for x in strikes if abs(x - spot) / spot <= keep_percent] 
    # print("after cut: ", strikes)
    strike_max = max(strikes)
    strike_min = min(strikes)
    df = df.loc[(strikes_se >= strike_min) & (strikes_se <= strike_max)].copy()
    # print(df)
    return df

def remove_dup_cut(df: pd.DataFrame, wide: bool):
    df = remove_dup_lines(df)
    if not wide:
        df = cut_off_degenerate_gambler(df, 0.2)
    return df

def smooth_oi_csv(df: pd.DataFrame, dsp_sec, ts_sigma_sec, strike_sigma_price):
    # 如果 Call 平仓表示跌，Call 开仓也表示跌，这个时候我们改用绝对值表示 Call 引发的波动，值得尝试。
    # df['oi_diff_c'] = np.abs(df['oi_diff_c'])
    # df['oi_diff_p'] = np.abs(df['oi_diff_p'])
    print(f'smooth: ts_sigma_sec={ts_sigma_sec}, strike_sigma_price={strike_sigma_price}')
    oi_c_1d, oi_c_2d = smooth_column(
            df,
            input_name='oi_diff_c',
            # input_name='oi_dlog_c',
            out1_name='oi_c_gau_ts', out2_name='oi_c_gau_2d',
            dsp_sec=dsp_sec, ts_sigma_sec=ts_sigma_sec, strike_sigma_price=strike_sigma_price)
    oi_p_1d, oi_p_2d = smooth_column(
            df,
            input_name='oi_diff_p',
            # input_name='oi_dlog_p',
            out1_name='oi_p_gau_ts', out2_name='oi_p_gau_2d',
            dsp_sec=dsp_sec, ts_sigma_sec=ts_sigma_sec, strike_sigma_price=strike_sigma_price)
    df['oi_diff_cp'] = df['oi_diff_c'] - df['oi_diff_p']
    # df['oi_dlog_cp'] = df['oi_dlog_c'] - df['oi_dlog_p']
    oi_cp_1d, oi_cp_2d = smooth_column(
            df,
            input_name='oi_diff_cp',
            # input_name='oi_dlog_cp',
            out1_name='oi_cp_gau_ts', out2_name='oi_cp_gau_2d',
            dsp_sec=dsp_sec, ts_sigma_sec=ts_sigma_sec, strike_sigma_price=strike_sigma_price)
    df_res = pd.concat([oi_c_1d, oi_c_2d, oi_p_1d, oi_p_2d, oi_cp_1d, oi_cp_2d], axis=1)
    # print(df)
    df_res['spotcode'] = df.loc[:, 'spotcode'].iloc[0]
    df_res['expirydate'] = df.loc[:, 'expirydate'].iloc[0]
    return df_res

def smooth_spot_df(df: pd.DataFrame, dsp_sec, ts_sigma_sec_list: list[int]):
    df = df.loc[:, ['dt', 'spotcode', 'spot_price']].drop_duplicates()
    df = df.sort_values('dt')
    df['spotcode'] = df['spotcode'].astype('str')
    se_ts = df.loc[:, ['dt']].astype('int64').values // 10**9

    for ts_sigma_sec in ts_sigma_sec_list:
        ts_wsize, ts_sigma, ts_diff_med = calc_window(se_ts, ts_sigma_sec, 3.5)
        print(f"spot: ts_sigma_sec={ts_sigma_sec}, ts_diff_med={ts_diff_med}, ts_wsize={ts_wsize}, ts_sigma={ts_sigma}")
        df[f'spot_price_{ts_sigma_sec}'] = left_gaussian(df['spot_price'], ts_wsize, ts_sigma)

    df = df.set_index('dt')
    df = downsample_time(df, dsp_sec)
    return df

# 这个是为了绘图编写的 DSP 函数的配置方案。
def dsp_file_2_plot(spot: str, suffix: str, strike_sigma: float, wide: bool):
    # 这个时间滤波窗口的大小根据做不同波段的因果验证可以有不同的调整。
    df = pd.read_csv(f'{DATA_DIR}/dsp_input/strike_oi_diff_{spot}_{suffix}.csv')
    df['dt'] = pd.to_datetime(df['dt'])
    df = remove_dup_cut(df, wide=wide)
    df_res = smooth_oi_csv(df, dsp_sec=120, ts_sigma_sec=1200, strike_sigma_price=strike_sigma)
    df_res.to_csv(f'{DATA_DIR}/dsp_plot/strike_oi_smooth_{spot}_{suffix}{gen_wide_suffix(wide)}.csv')
    df_spot = smooth_spot_df(df, dsp_sec=120, ts_sigma_sec_list=[300])
    df_spot.to_csv(f'{DATA_DIR}/dsp_plot/spot_{spot}_{suffix}{gen_wide_suffix(wide)}.csv')


# 这个是为了计算不同长度的相关关系做的 dsp
def dsp_file_2_intersect(spot: str, suffix: str,
                        ts_sigma_list: list[int], strike_sigma_list: list[float],
                        wide: bool):
    df = pd.read_csv(f'{DATA_DIR}/dsp_input/strike_oi_diff_{spot}_{suffix}.csv')
    df['dt'] = pd.to_datetime(df['dt'])
    df = remove_dup_cut(df, wide=wide)
    for ts_sigma in ts_sigma_list:
        for strike_sigma in strike_sigma_list:
            df_res = smooth_oi_csv(df, dsp_sec=60, ts_sigma_sec=ts_sigma, strike_sigma_price=strike_sigma)
            df_res.to_csv(f'{DATA_DIR}/dsp_conv/strike_oi_smooth_{spot}_{suffix}{gen_wide_suffix(wide)}_{ts_sigma}_{strike_sigma}.csv')
    df_spot = smooth_spot_df(df, dsp_sec=60, ts_sigma_sec_list=ts_sigma_list)
    df_spot.to_csv(f'{DATA_DIR}/dsp_conv/spot_{spot}_{suffix}{gen_wide_suffix(wide)}.csv')

def calc_dsp_surface(spot: str, suffix: str, wide: bool):
    spot_config = get_spot_config(spot)
    strike_sigmas = spot_config.get_strike_sigmas(wide)
    dsp_file_2_plot(spot, suffix,
            strike_sigma=strike_sigmas[1],
            wide=wide)

def calc_dsp_intersects(spot: str, suffix: str, wide: bool):
    spot_config = get_spot_config(spot)
    strike_sigmas = spot_config.get_strike_sigmas(wide)
    dsp_file_2_intersect(spot, suffix,
            spot_config.oi_ts_gaussian_sigmas,
            strike_sigmas,
            wide=wide,
    )
    
def main(spot: str, suffix: str, wide: bool):
    calc_dsp_surface(spot, suffix, wide=wide)
    calc_dsp_intersects(spot, suffix, wide=wide)

@click.command()
@click.option('-s', '--spot', type=str, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, help="csv file name suffix.")
@click.option('--wide', type=bool, default=False, help="use wide strike sigma")
def click_main(spot: str, suffix: str, wide: bool):
    main(spot, suffix, wide=wide)

if __name__ == '__main__':
    click_main()
    pass

