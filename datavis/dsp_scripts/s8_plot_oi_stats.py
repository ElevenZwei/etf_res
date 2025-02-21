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

from dsp_config import DATA_DIR, get_spot_config, plot_dt_str

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
    for ts in spot_config.oi_ts_gaussian_sigmas:
        spearman_col = f'oi_cp_spearman_ts_{ts}'
        if spearman_col not in df.columns:
            continue
        fig.add_trace(go.Scatter(x=x_ts_uni, y=df[spearman_col], mode='lines', name=f'spearman ts {ts}'),
                row=row, col=col)
    return fig

def plot_sigma_spearman(df: pd.DataFrame, spot: str, fig, row, col):
    spot_config = get_spot_config(spot)
    x_ts_uni = df['dt']
    spearman_cols = [x for x in df.columns if x.startswith('oi_cp_spearman_sigma_')]
    for sigma in spot_config.oi_strike_gaussian_sigmas:
        spearman_col = f'oi_cp_spearman_sigma_{sigma}'
        if spearman_col not in df.columns:
            continue
        fig.add_trace(go.Scatter(x=x_ts_uni, y=df[spearman_col], mode='lines', name=f'spearman sigma {sigma}'),
                row=row, col=col)
    return fig

def plot_stats(df: pd.DataFrame):
    fig = subplots.make_subplots(rows=3, cols=1,
        row_heights=[0.6, 0.2, 0.2],
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=[
            'OI CP',
            'OI CP Ts Spearman',
            'OI CP Sigma Spearman',
        ]
    )
    fig.update_layout(
        height=1500,
        width=1400,
        title_text="OI Stats",
        autosize=True,
        margin=dict(t=40, b=40),
    )
    df = plot_dt_str(df, 'dt')
    fig = plot_oi(df, '159915', False, fig, 1, 1)
    fig = plot_ts_spearman(df, '159915', fig, 2, 1)
    fig = plot_sigma_spearman(df, '159915', fig, 3, 1)
    return fig

def plot_file(spot: str, suffix: str):
    df = pd.read_csv(DATA_DIR / 'dsp_conv' / f'stats_{spot}_{suffix}.csv')
    fig = plot_stats(df)
    fig.show()

@click.command()
@click.option('-s', '--spot', type=str, required=True, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, required=True, help="csv file name suffix.")
def click_main(spot: str, suffix: str):
    plot_file(spot, suffix)    

if __name__ == '__main__':
    click_main()

