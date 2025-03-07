"""
这个脚本绘制一下 trade stats 的上下震荡和分布情况。
"""

import datetime
import glob
import click
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.colors as pc
import plotly.subplots as subplots

from dsp_config import DATA_DIR

# 把 10:15 之前开仓的取出来再画一画。
# 计算一下不同时间开仓的收益情况。
# 计算一下平仓的收益分位数。
# 对于突刺的情况可以做针对性的优化，因为插针在均线后的指标上反映太慢了，所以用瞬间止盈的做法更好。
# 例如如果在 20 分钟以内达到 x% 的收益那么立刻平仓。这是对于三月这样的消息面行情的一种把握方法。
# 在点阵图上的先验后验关系就是 30minute_max 和 close 。

# 另外昨天说到对于 OI 的突刺现象需要人工干预平仓，或者说太强烈的突刺需要另外的信号传送，这个我不太清楚怎么量化，我需要统计这个现象出现的历史时间。
# Good!

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
            'Max Close Early Open',
            'Max Min',
        ])
    fig.update_layout(
        height=1000,
        width=1400,
        title_text=f'Trade Stats For {wildcard}',
        margin=dict(t=80, b=80),
    )
    df_early = df[df['open_dt'].dt.time <= datetime.time(10, 0)]
    fig = plot_xy(df, 'pnl_max', 'pnl', 'label', fig, 1, 1)
    fig = plot_xy(df_early, 'pnl_max', 'pnl', 'label', fig, 2, 1)
    fig = plot_xy(df, 'pnl_min', 'pnl', 'label', fig, 1, 2)
    fig = plot_xy(df, 'pnl_max', 'pnl_min', 'label', fig, 2, 2)
    return fig
    
def read_df(wildcard: str):
    fs = glob.glob(f'{DATA_DIR}/dsp_stats/{wildcard}.csv')
    dfs = [pd.read_csv(f) for f in fs]
    df = pd.concat(dfs)
    df['label'] = df['open_dt'] + '_' + df['close_dt']
    df['open_dt'] = pd.to_datetime(df['open_dt'])
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
