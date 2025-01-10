"""
对于期权 OI 很多曲面的交叉线做一个二维绘图。
"""

from collections import defaultdict
import click
import plotly.graph_objects as go
import plotly.colors as pc
import numpy as np
import pandas as pd

from dsp_config import DATA_DIR, gen_wide_suffix, get_spot_config

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
    # return (arr - arr.mean()) / arr.std()
    return (arr - arr[0]) / (arr[0] * 0.01)

# OI 使用 / std 和 / 1000 在曲线之间的关系上会产生区别。
def standard_oi_diff(arr: np.array, zoom: int):
    # return (arr - arr[0])
    return (arr - arr[0]) / zoom
    # return (arr - arr.mean()) / arr.std()

def plot_df(df: pd.DataFrame, spot: str, title: str, wide: bool):
    spot_config = get_spot_config(spot)
    df['dt'] = pd.to_datetime(df['dt'])

    # clip morning.
    # df = df.loc[df['dt'].dt.time < datetime.time(13)].copy()

    df.loc[:, 'sdt'] = df['dt'].apply(lambda x: x.strftime('%m-%d %H:%M:%S'))
    df = df.drop(columns=['dt'])
    df['dt'] = df['sdt']
    x_ts_uni = df['dt']

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

    line_plot = go.Figure()
    line_plot.add_trace(go.Scatter(x=x_ts_uni, y=y_spot, mode='lines', name='spot', line={'color': PLOT_CONFIG['spot_color_seq'][1]}))
    line_plot.add_trace(go.Scatter(x=x_ts_uni, y=y_spot_300, mode='lines', name='spot 300', line={'color': PLOT_CONFIG['spot_color_seq'][4]}))

    for strike_id, strike_sigma in enumerate(strike_sigmas):
        for ts_id, ts_sigma in enumerate(spot_config.oi_ts_gaussian_sigmas):
            line_plot.add_trace(go.Scatter(
                x=x_ts_uni,
                y=oi_pc_series[strike_sigma][ts_sigma],
                mode='lines',
                name=f'pc {ts_sigma} {strike_sigma}',
                line={'color': PLOT_CONFIG['oi_color_seqs'][strike_id][ts_id+1]}))

    line_plot.update_layout(
        title=f'{title} OI Intersects',
        xaxis_title='Time',
        yaxis_title='OI',
    )
    return line_plot

def plot_file(spot: str, suffix: str, save: bool, show: bool, wide: bool):
    wide_suffix = gen_wide_suffix(wide)
    suffix = f'{suffix}{wide_suffix}'
    df = pd.read_csv(f'{DATA_DIR}/dsp_conv/merged_{spot}_{suffix}.csv')
    line_plot = plot_df(df, spot=spot, title=f"{spot} {suffix}", wide=wide)
    if show:
        line_plot.show()
    if save:
        line_plot.write_html(f'{DATA_DIR}/html_oi/oi_intersect_{spot}_{suffix}.html')
        line_plot.write_image(f'{DATA_DIR}/png_oi/oi_intersect_{spot}_{suffix}.png',
                              width=1200, height=800)

def main(spot: str, suffix: str, save: bool, show: bool, wide: bool):
    plot_file(spot, suffix, save=save, show=show, wide=wide)

@click.command()
@click.option('-s', '--spot', type=str, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, help="csv file name suffix.")
@click.option('--save', type=bool, default=True, help="save to html.")
@click.option('--show', type=bool, default=True, help="show plot.")
@click.option('--wide', type=bool, default=False, help="wide plot.")
def click_main(spot: str, suffix: str, save: bool, show: bool, wide: bool):
    main(spot, suffix, save, show, wide=wide)

if __name__ == '__main__':
    click_main()
    pass
