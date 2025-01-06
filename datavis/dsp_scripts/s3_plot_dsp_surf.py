# 这个脚本读取 dsp_plot 里面经过了插值和滤波的数据，然后绘制更加平滑的 oi 曲面，
# 包括红色的 call 曲面，蓝色的 put 曲面，和黄绿色的 call - put 曲面。

import click
import plotly.graph_objects as go
import numpy as np
import pandas as pd

from dsp_config import DATA_DIR

def plot_df(df: pd.DataFrame, title: str):
    df['dt'] = pd.to_datetime(df['dt']).apply(lambda x: x.strftime('%m-%d %H:%M:%S'))
    x_uni = np.sort(df['dt'].unique())
    y_uni = np.sort(df['strike'].unique())
    x_grid, y_grid = np.meshgrid(x_uni, y_uni)
    # zero_grid = np.zeros_like(x_grid)
    # zero_surf = go.Surface(x=x_grid, y=y_grid, z=zero_grid,
    #         opacity=0.2, showlegend=False, showscale=False, hoverinfo='none',
    #         contours=go.surface.Contours(
    #                 x=go.surface.contours.X(highlight=False),
    #                 y=go.surface.contours.Y(highlight=False),
    #                 z=go.surface.contours.Z(highlight=False),),
    # )

    contours_no_z = go.surface.Contours(
        x=go.surface.contours.X(highlight=True),
        y=go.surface.contours.Y(highlight=True),
        z=go.surface.contours.Z(highlight=False),
    )

    # 这个是时间均值的曲面
    # cp_grid_ts = df.pivot(index='strike', columns='dt', values='oi_cp_gau_ts').values / 10
    # cp_surf_ts = go.Surface(x=x_grid, y=y_grid, z=cp_grid_ts,
    #         cmin=color_min, cmax=color_max,
    #         colorscale='Viridis', opacity=0.3, contours=contours_no_z)

    # 这个是时间和行权价格均值的曲面
    c_grid_2d = df.pivot(index='strike', columns='dt', values='oi_c_gau_2d').values
    p_grid_2d = df.pivot(index='strike', columns='dt', values='oi_p_gau_2d').values
    cp_grid_2d = df.pivot(index='strike', columns='dt', values='oi_cp_gau_2d').values
    z_max = np.max([np.max(c_grid_2d), np.max(p_grid_2d)])
    z_min = np.min([np.min(c_grid_2d), np.min(p_grid_2d)])

    color_min, color_max=[z_min, z_max]
    opac = 0.9
    c_surf_2d = go.Surface(x=x_grid, y=y_grid, z=c_grid_2d, name='c_2d',
            cmin=color_min, cmax=color_max,
            colorscale='reds', opacity=opac, contours=contours_no_z)
    p_surf_2d = go.Surface(x=x_grid, y=y_grid, z=p_grid_2d, name='p_2d',
            cmin=color_min, cmax=color_max,
            colorscale='blues', opacity=opac, contours=contours_no_z)
    cp_surf_2d = go.Surface(x=x_grid, y=y_grid, z=cp_grid_2d, name='cp_2d',
            # cmin=color_min, cmax=color_max,
            colorscale='Viridis', opacity=opac, contours=contours_no_z)

    layout = go.Layout(
        scene=go.layout.Scene(
            xaxis_title='Time',
            yaxis_title='Strike',
            zaxis_title='OiDiff',
            xaxis = go.layout.scene.XAxis(showspikes=False),
            yaxis = go.layout.scene.YAxis(showspikes=False),
            zaxis = go.layout.scene.ZAxis(showspikes=False),
        ),
        title=f"{title} Strike OiDiff"
    )

    fig = go.Figure(layout=layout)
    fig.add_trace(c_surf_2d)
    fig.add_trace(p_surf_2d)
    fig.add_trace(cp_surf_2d)
    # fig.add_trace(cp_surf_ts)

    return fig

def plot_file(spot: str, date: str, show: bool, save: bool):
    df = pd.read_csv(f'{DATA_DIR}/dsp_plot/strike_oi_smooth_{spot}_{date}.csv')
    figure = plot_df(df, title=f"{spot} {date}")
    if show:
        figure.show()
    if save:
        figure.write_html(f'{DATA_DIR}/html_oi/oi_smooth_{spot}_{date}.html')
        figure.write_image(f'{DATA_DIR}/png_oi/oi_smooth_{spot}_{date}.png',
                           width=1200, height=800)

def main(spot: str, suffix: str, show: bool = True, save: bool = True):
    plot_file(spot, suffix, show=show, save=save)

@click.command()
@click.option('-s', '--spot', type=str, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, help="csv file name suffix.")
@click.option('--show', type=bool, default=True, help="show plot.")
@click.option('--save', type=bool, default=True, help="save to html.")
def click_main(spot: str, suffix: str, save: bool, show: bool):
    main(spot, suffix=suffix, save=save, show=show)

if __name__ == '__main__':
    click_main()
    pass
