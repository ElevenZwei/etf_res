"""
这个脚本绘制一下 trade stats 的上下震荡和分布情况。
"""

import glob
import click
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.colors as pc
import plotly.subplots as subplots

from dsp_config import DATA_DIR

def plot_xy(df: pd.DataFrame, x_col: str, y_col: str, hover_col: str, fig, row, col):
    x_uni = df[x_col]
    y_uni = df[y_col]
    hover_uni = df[hover_col]
    fig.add_trace(
            go.Scatter(x=x_uni, y=y_uni, mode='markers',
                    text=hover_uni, hoverinfo='text',
                    name=f'{x_col}_{y_col}'),
            row=row, col=col)
    return fig

def plot_df(df: pd.DataFrame, wildcard: str):
    fig = subplots.make_subplots(rows=2, cols=2,
        vertical_spacing=0.04,
        subplot_titles=[
            'Max Close',
            'Min Close',
            'Max Min',
        ])
    fig.update_layout(
        height=1000,
        width=1400,
        title_text=f'Trade Stats For {wildcard}',
        margin=dict(t=80, b=80),
    )
    fig = plot_xy(df, 'pnl_max', 'pnl', 'label', fig, 1, 1)
    fig = plot_xy(df, 'pnl_min', 'pnl', 'label', fig, 1, 2)
    fig = plot_xy(df, 'pnl_max', 'pnl_min', 'label', fig, 2, 1)
    return fig
    
def read_df(wildcard: str):
    fs = glob.glob(f'{DATA_DIR}/dsp_stats/{wildcard}.csv')
    dfs = [pd.read_csv(f) for f in fs]
    df = pd.concat(dfs)
    df['label'] = df['open_dt'] + '_' + df['close_dt']
    return df

def main(wildcard: str):
    df = read_df(wildcard)
    fig = plot_df(df, wildcard)
    fig.show()

@click.command()
@click.option('-w', '--wildcard', type=str, required=True)
def click_main(wildcard: str):
    main(wildcard)

if __name__ == '__main__':
    click_main()
