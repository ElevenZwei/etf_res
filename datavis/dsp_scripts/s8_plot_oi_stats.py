"""
这个脚本的作用是把 s7 里面计算的统计信息画出来，
人工检验一下，然后发现其中的规律。
"""

from collections import defaultdict

import click
import pandas as pd
import numpy as np
from scipy import stats
import plotly.graph_objects as go
import plotly.colors as pc
import plotly.subplots as subplots

from dsp_config import DATA_DIR, get_spot_config, plot_dt_str, gen_wide_suffix

PLOT_CONFIG = {
    'spot_color_seq': pc.sequential.tempo,
    'oi_color_seqs': [
        pc.sequential.Peach,
        pc.sequential.Burg,
        pc.sequential.Magenta,
        pc.sequential.Purp,
        pc.sequential.Teal,
    ],
}

def standard_prices(arr: np.array):
    return (arr - arr[0]) / (arr[0] * 0.01)

def standard_oi_diff(arr: np.array, zoom: int):
    return (arr - arr[0]) / zoom

def plot_oi(df: pd.DataFrame, spot: str, wide: bool, fig, row, col):
    x_ts_uni = df['dt']
    spot_config = get_spot_config(spot)

    # structure: {strike_sigma: {ts_sigma: series}}
    oi_pc_series = defaultdict(lambda: defaultdict())
    strike_sigmas = spot_config.get_strike_sigmas(wide)
    for strike_sigma in strike_sigmas:
        for ts_sigma in spot_config.oi_ts_gaussian_sigmas:
            oi_pc_series[strike_sigma][ts_sigma] = (-1 * standard_oi_diff(
                    arr=df.loc[:, f'oi_cp_{ts_sigma}_{strike_sigma}'].values,
                    zoom=spot_config.oi_plot_intersect_zoom)
            )

    y_spot = standard_prices(df.loc[:, 'spot_price'].values)
    y_spot_300 = standard_prices(df.loc[:, 'spot_price_300'].values)
    fig.add_trace(
            go.Scatter(x=x_ts_uni, y=y_spot, mode='lines', name='spot',
                    line={'color': PLOT_CONFIG['spot_color_seq'][1]}),
            row=row, col=col)
    fig.add_trace(
            go.Scatter(x=x_ts_uni, y=y_spot_300, mode='lines', name='spot 300',
                    line={'color': PLOT_CONFIG['spot_color_seq'][4]}),
            row=row, col=col)

    for strike_id, strike_sigma in enumerate(strike_sigmas):
        for ts_id, ts_sigma in enumerate(spot_config.oi_ts_gaussian_sigmas):
            fig.add_trace(go.Scatter(
                    x=x_ts_uni,
                    y=oi_pc_series[strike_sigma][ts_sigma],
                    mode='lines',
                    name=f'pc {ts_sigma} {strike_sigma}',
                    line={'color': PLOT_CONFIG['oi_color_seqs'][strike_id][ts_id+1]}),
                    row=row, col=col)
    return fig

def plot_ts_spearman(df: pd.DataFrame, spot: str, fig, row, col):
    spot_config = get_spot_config(spot)
    x_ts_uni = df['dt']
    spearman_cols = [x for x in df.columns if x.startswith('oi_cp_spearman_ts_')]
    for ts_id, ts in enumerate(spot_config.oi_ts_gaussian_sigmas):
        spearman_col = f'oi_cp_spearman_ts_{ts}'
        if spearman_col not in df.columns:
            continue
        fig.add_trace(
                go.Scatter(
                        x=x_ts_uni, y=df[spearman_col], mode='lines', name=f'spearman ts {ts}',
                        line={'color': PLOT_CONFIG['oi_color_seqs'][3][ts_id+1]}),
                row=row, col=col)
    return fig

def plot_sigma_spearman(df: pd.DataFrame, spot: str, fig, row, col):
    spot_config = get_spot_config(spot)
    x_ts_uni = df['dt']
    spearman_cols = [x for x in df.columns if x.startswith('oi_cp_spearman_sigma_')]
    for sigma_id, sigma in enumerate(spot_config.oi_strike_gaussian_sigmas):
        spearman_col = f'oi_cp_spearman_sigma_{sigma}'
        if spearman_col not in df.columns:
            continue
        fig.add_trace(
                go.Scatter(
                        x=x_ts_uni, y=df[spearman_col], mode='lines', name=f'spearman sigma {sigma}',
                        line={'color': PLOT_CONFIG['oi_color_seqs'][sigma_id][2]}),
                row=row, col=col)
    return fig

def plot_ts_stdev(df: pd.DataFrame, spot: str, fig, row, col):
    spot_config = get_spot_config(spot)
    x_ts_uni = df['dt']
    stdev_cols = [x for x in df.columns if x.startswith('oi_cp_stdev_ts_')]
    for ts_id, ts in enumerate(spot_config.oi_ts_gaussian_sigmas):
        stdev_col = f'oi_cp_stdev_ts_{ts}'
        if stdev_col not in df.columns:
            continue
        fig.add_trace(
                go.Scatter(
                        x=x_ts_uni, y=df[stdev_col], mode='lines', name=f'stdev ts {ts}',
                        line={'color': PLOT_CONFIG['oi_color_seqs'][3][ts_id+1]}),
                row=row, col=col)
    return fig

def plot_sigma_stdev(df: pd.DataFrame, spot: str, fig, row, col):
    spot_config = get_spot_config(spot)
    x_ts_uni = df['dt']
    stdev_cols = [x for x in df.columns if x.startswith('oi_cp_stdev_sigma_')]
    for sigma_id, sigma in enumerate(spot_config.oi_strike_gaussian_sigmas):
        stdev_col = f'oi_cp_stdev_sigma_{sigma}'
        if stdev_col not in df.columns:
            continue
        fig.add_trace(
                go.Scatter(
                        x=x_ts_uni, y=df[stdev_col], mode='lines', name=f'stdev sigma {sigma}',
                        line={'color': PLOT_CONFIG['oi_color_seqs'][sigma_id][2]}),
                row=row, col=col)
    return fig

def plot_ts_dirstd(df: pd.DataFrame, spot: str, fig, row, col):
    spot_config = get_spot_config(spot)
    x_ts_uni = df['dt']
    spearman_cols = [x for x in df.columns if x.startswith('oi_cp_spearman_ts_')]
    stdev_cols = [x for x in df.columns if x.startswith('oi_cp_stdev_ts_')]
    for ts_id, ts in enumerate(spot_config.oi_ts_gaussian_sigmas):
        spearman_col = f'oi_cp_spearman_ts_{ts}'
        stdev_col = f'oi_cp_stdev_ts_{ts}'
        if spearman_col not in df.columns or stdev_col not in df.columns:
            continue
        fig.add_trace(
                go.Scatter(
                        x=x_ts_uni, y=df[spearman_col] * df[stdev_col], mode='lines', name=f'dirstd ts {ts}',
                        line={'color': PLOT_CONFIG['oi_color_seqs'][3][ts_id+1]}),
                row=row, col=col)
    return fig

def plot_sigma_dirstd(df: pd.DataFrame, spot: str, fig, row, col):
    spot_config = get_spot_config(spot)
    x_ts_uni = df['dt']
    spearman_cols = [x for x in df.columns if x.startswith('oi_cp_spearman_sigma_')]
    stdev_cols = [x for x in df.columns if x.startswith('oi_cp_stdev_sigma_')]
    for sigma_id, sigma in enumerate(spot_config.oi_strike_gaussian_sigmas):
        spearman_col = f'oi_cp_spearman_sigma_{sigma}'
        stdev_col = f'oi_cp_stdev_sigma_{sigma}'
        if spearman_col not in df.columns or stdev_col not in df.columns:
            continue
        fig.add_trace(
                go.Scatter(
                        x=x_ts_uni, y=df[spearman_col] * df[stdev_col], mode='lines', name=f'dirstd sigma {sigma}',
                        line={'color': PLOT_CONFIG['oi_color_seqs'][sigma_id][2]}),
                row=row, col=col)
    return fig

def plot_price(df: pd.DataFrame, fig, row, col):
    x_ts_uni = df['dt']
    y_spot = standard_prices(df['spot_price'].values)
    fig.add_trace(
            go.Scatter(x=x_ts_uni, y=y_spot, mode='lines', name='spot',
                    line={'color': PLOT_CONFIG['spot_color_seq'][2]}),
            row=row, col=col)
    return fig

def plot_trade_pos(df: pd.DataFrame, fig, row, col):
    x_ts_uni = df['dt']
    y_ts_pos = df['ts_pos']
    y_sigma_pos = df['sigma_pos']
    fig.add_trace(
            go.Scatter(x=x_ts_uni, y=y_ts_pos, mode='lines', name='ts pos',
                    line={'color': PLOT_CONFIG['spot_color_seq'][3]}),
            row=row, col=col)
    fig.add_trace(
            go.Scatter(x=x_ts_uni, y=y_sigma_pos, mode='lines', name='sigma pos',
                    line={'color': PLOT_CONFIG['spot_color_seq'][4]}),
            row=row, col=col)
    return fig

def plot_stats(df: pd.DataFrame):
    fig = subplots.make_subplots(rows=8, cols=1,
        row_heights=[0.3, 0.22, 0.1, 0.1, 0.07, 0.07, 0.07, 0.07],
        shared_xaxes=True,
        vertical_spacing=0.04,
        subplot_titles=[
            'OI CP',
            'Trade Pos',
            'OI CP Ts DirStd',
            'OI CP Sigma DirStd',
            'OI CP Ts Spearman',
            'OI CP Ts Stdev',
            'OI CP Sigma Spearman',
            'OI CP Sigma Stdev',
        ]
    )
    fig.update_layout(
        height=2500,
        width=1400,
        title_text="OI Stats",
        autosize=True,
        margin=dict(t=40, b=40),
        # hovermode='x unified',
        # legend_traceorder='normal',
    )
    # fig.update_traces(xaxis='x1')
    df = plot_dt_str(df, 'dt')
    fig = plot_oi(df, '159915', False, fig, 1, 1)
    fig = plot_trade_pos(df, fig, 2, 1)
    fig = plot_price(df, fig, 2, 1)
    fig = plot_ts_dirstd(df, '159915', fig, 3, 1)
    fig = plot_sigma_dirstd(df, '159915', fig, 4, 1)
    fig = plot_ts_spearman(df, '159915', fig, 5, 1)
    fig = plot_ts_stdev(df, '159915', fig, 6, 1)
    fig = plot_sigma_spearman(df, '159915', fig, 7, 1)
    fig = plot_sigma_stdev(df, '159915', fig, 8, 1)
    return fig

def plot_file(spot: str, suffix: str, save: bool, show: bool, wide: bool):
    suffix = suffix + gen_wide_suffix(wide)
    df = pd.read_csv(DATA_DIR / 'dsp_conv' / f'stats_{spot}_{suffix}.csv')
    fig = plot_stats(df)
    if show:
        fig.show()
    if save:
        fig.write_html(DATA_DIR / 'html_oi' / f'oi_stats_{spot}_{suffix}.html')
        fig.write_image(DATA_DIR / 'png_oi' / f'oi_stats_{spot}_{suffix}.png')

def main(spot: str, suffix: str, show: bool, save: bool, wide: bool):
    plot_file(spot, suffix, save=save, show=show, wide=wide)

@click.command()
@click.option('-s', '--spot', type=str, required=True, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, required=True, help="csv file name suffix.")
@click.option('--save', type=bool, default=True, help="save to html.")
@click.option('--show', type=bool, default=True, help="show plot.")
@click.option('--wide', type=bool, default=False, help="wide plot.")
def click_main(spot: str, suffix: str, show: bool, save: bool, wide: bool):
    plot_file(spot, suffix, save=save, show=show, wide=wide)

if __name__ == '__main__':
    click_main()

