import click
import plotly.graph_objects as go
import numpy as np
import pandas as pd

from dsp_config import DATA_DIR, get_spot_config, gen_wide_suffix, plot_dt_str
from s4_plot_dsp_inter import standard_oi_diff, standard_prices

def plot_df(df: pd.DataFrame, spot_df: pd.DataFrame, title: str,
        spot_ts: int, zoom: int, cp_only: bool):
    df = plot_dt_str(df)
    x_uni = np.sort(df['dt'].unique())
    y_uni = np.sort(df['sigma'].unique())
    x_grid, y_grid = np.meshgrid(x_uni, y_uni)

    # settings
    zero_grid = np.zeros_like(x_grid)
    zero_surf = go.Surface(x=x_grid, y=y_grid, z=zero_grid,
            opacity=0.4, showlegend=False, showscale=False,
            colorscale='BuPu',
            hoverinfo='none',
            contours=go.surface.Contours(
                    x=go.surface.contours.X(highlight=False),
                    y=go.surface.contours.Y(highlight=False),
                    z=go.surface.contours.Z(highlight=False),),
    )
    coutours_set = go.surface.Contours(
        x=go.surface.contours.X(highlight=True),
        y=go.surface.contours.Y(highlight=True),
        z=go.surface.contours.Z(highlight=True),
    )

    # put - call
    if not cp_only:
        c_grid_2d = df.pivot(index='sigma', columns='dt', values='oi_c_mean').values
        c_grid_2d = standard_oi_diff(c_grid_2d.transpose(), zoom).transpose()
        p_grid_2d = df.pivot(index='sigma', columns='dt', values='oi_p_mean').values
        p_grid_2d = standard_oi_diff(p_grid_2d.transpose(), zoom).transpose()
        z_max = np.max([np.max(c_grid_2d), np.max(p_grid_2d)])
        z_min = np.min([np.min(c_grid_2d), np.min(p_grid_2d)])
        color_min, color_max=[z_min, z_max]
        opac = 0.6
        c_surf_2d = go.Surface(x=x_grid, y=y_grid, z=c_grid_2d, name='c_2d',
                cmin=color_min, cmax=color_max,
                colorscale='Sunset', colorbar=dict(x=1.1),
                opacity=opac, contours=coutours_set)
        p_surf_2d = go.Surface(x=x_grid, y=y_grid, z=p_grid_2d, name='p_2d',
                cmin=color_min, cmax=color_max,
                colorscale='Emrld', colorbar=dict(x=1.2),
                opacity=opac, contours=coutours_set)

    cp_grid_2d = -1 * df.pivot(index='sigma', columns='dt', values='oi_cp_mean').values
    cp_grid_2d = standard_oi_diff(cp_grid_2d.transpose(), zoom).transpose()
    cp_surf_2d = go.Surface(x=x_grid, y=y_grid, z=cp_grid_2d, name='cp_2d',
            # cmin=color_min, cmax=color_max,
            colorscale='Jet', colorbar=dict(x=1.3),
            opacity=1, contours=coutours_set)
    
    spot_df = plot_dt_str(spot_df)
    spot_x_uni = spot_df['dt']
    spot_z_uni = standard_prices(spot_df[f'spot_price_{spot_ts}'].values)
    y_line = np.full_like(spot_x_uni, fill_value=y_uni[0])
    spot_curve = go.Scatter3d(x=spot_x_uni, y=y_line, z=spot_z_uni,
            mode='lines', line=dict(color='black', width=5),
            name=f'spot_curve_{spot_ts}',
            opacity=0.8,
    )
    spot_x_grid, spot_y_grid = np.meshgrid(spot_x_uni, y_uni)
    spot_z_grid = np.tile(spot_z_uni, (len(y_uni), 1))
    spot_surf = go.Surface(x=spot_x_grid, y=spot_y_grid, z=spot_z_grid,
            name=f'spot_surf_{spot_ts}',
            opacity=0.2, colorscale='Plasma', colorbar=dict(x=1.4),
            contours=go.surface.Contours(
                    x=go.surface.contours.X(highlight=False),
                    y=go.surface.contours.Y(highlight=True),
                    z=go.surface.contours.Z(highlight=False),),
    )

    camera_rotate = np.pi * -0.65
    camera_eye = dict(x=1.25 * np.cos(camera_rotate) - 1.25 * np.sin(camera_rotate),
                      y=1.25 * np.sin(camera_rotate) + 1.25 * np.cos(camera_rotate),
                      z=0.3)
    layout = go.Layout(
        scene=go.layout.Scene(
            xaxis_title='Time',
            yaxis_title='Sigma',
            zaxis_title='OiDiff',
            aspectmode='cube',
            xaxis = go.layout.scene.XAxis(showspikes=False),
            yaxis = go.layout.scene.YAxis(showspikes=False),
            zaxis = go.layout.scene.ZAxis(showspikes=False),
            camera = go.layout.scene.Camera(eye=camera_eye),
        ),
        title=f'{title} Oi Mean Surface',
    )

    fig = go.Figure(data=[
        zero_surf,
        cp_surf_2d,
        spot_curve,
        spot_surf,
    ], layout=layout)

    if not cp_only:
        fig.add_trace(c_surf_2d)
        fig.add_trace(p_surf_2d)

    return fig

def plot_file(spot: str, suffix: str, show: bool, save: bool):
    oi_df = pd.read_csv(f'{DATA_DIR}/dsp_conv/oi_surface_{spot}_{suffix}.csv')
    spot_df = pd.read_csv(f'{DATA_DIR}/dsp_conv/spot_{spot}_{suffix}.csv')
    spot_config = get_spot_config(spot)
    figure = plot_df(oi_df, spot_df,
            title=f'{spot} {suffix}',
            spot_ts=spot_config.oi_ts_gaussian_sigmas[1],
            zoom=spot_config.oi_plot_intersect_zoom,
            cp_only=False)
    if show:
        figure.show()
    if save:
        figure.write_html(f'{DATA_DIR}/html_oi/oi_surface_{spot}_{suffix}.html')
        figure.write_image(f'{DATA_DIR}/png_oi/oi_surface_{spot}_{suffix}.png',
                           width=1200, height=800)

def main(spot: str, suffix: str, show: bool, save: bool):
    plot_file(spot, suffix, show=show, save=save)

@click.command()
@click.option('-s', '--spot', type=str, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, help="csv file name suffix.")
@click.option('--show', type=bool, default=True, help="show plot.")
@click.option('--save', type=bool, default=True, help="save to html.")
def click_main(spot: str, suffix: str, show: bool, save: bool):
    main(spot, suffix, show=show, save=save)

if __name__ == '__main__':
    click_main()
