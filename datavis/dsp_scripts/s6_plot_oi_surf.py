import click
import plotly.graph_objects as go
import numpy as np
import pandas as pd

from dsp_config import DATA_DIR, gen_wide_suffix

def plot_df(df: pd.DataFrame, title: str):
    df['dt'] = pd.to_datetime(df['dt']).apply(lambda x: x.strftime('%m-%d %H:%M:%S'))
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
    c_grid_2d = df.pivot(index='sigma', columns='dt', values='oi_c_mean').values
    p_grid_2d = df.pivot(index='sigma', columns='dt', values='oi_p_mean').values
    cp_grid_2d = -1 * df.pivot(index='sigma', columns='dt', values='oi_cp_mean').values
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
    cp_surf_2d = go.Surface(x=x_grid, y=y_grid, z=cp_grid_2d, name='cp_2d',
            # cmin=color_min, cmax=color_max,
            colorscale='Jet', colorbar=dict(x=1.3),
            opacity=1, contours=coutours_set)
    
    layout = go.Layout(
        scene=go.layout.Scene(
            xaxis_title='Time',
            yaxis_title='Sigma',
            zaxis_title='OiDiff',
            aspectmode='cube',
            xaxis = go.layout.scene.XAxis(showspikes=False),
            yaxis = go.layout.scene.YAxis(showspikes=False),
            zaxis = go.layout.scene.ZAxis(showspikes=False),
            camera = go.layout.scene.Camera(eye=dict(x=1.25, y=-1.25, z=0.5)),
        ),
        title=f'{title} Oi Mean Surface',
    )

    fig = go.Figure(data=[
        zero_surf,
        # c_surf_2d, p_surf_2d,
        cp_surf_2d
    ], layout=layout)

    return fig

def plot_file(spot: str, suffix: str, show: bool, save: bool):
    df = pd.read_csv(f'{DATA_DIR}/dsp_conv/oi_surface_{spot}_{suffix}.csv')
    figure = plot_df(df, f'{spot} {suffix}')
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
