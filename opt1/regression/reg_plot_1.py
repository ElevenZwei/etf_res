"""
Use this script to plot the regression results of the model.
Use plotly to plot 'real' and 'pred' as lines.
"""

import click
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

def read_csv(fpath: str):
    """
    读取 CSV 文件，并返回数据框。
    :param fpath: CSV 文件路径
    :return: 数据框
    """
    df = pd.read_csv(fpath)
    df['dt'] = pd.to_datetime(df['dt'])
    df = df.sort_values(by='dt')
    df['dtstr'] = df['dt'].apply(lambda x: x.strftime("%m-%d %H:%M:%S"))
    return df


def plot_df_lines(df: pd.DataFrame, dt: datetime.date):
    """
    Use plotly.express.
    'dtstr' as x axis.
    'real' and 'pred' as y axis.
    """
    df['neg_residual'] = -df['residual']
    fig = px.line(df, x='dtstr', y=['real', 'pred',
                    # 'residual',
                    'neg_residual'
                    ],
                  title=f'Regression Result {dt}')
    fig.update_layout(xaxis_title='Date', yaxis_title='Value')
    fig.show()
    return fig


def main(dt: datetime.date, name: str):
    df = read_csv(f'../output/{dt.strftime('%Y%m%d')}/pred_{name}.csv')
    fig = plot_df_lines(df, dt)


@click.command()
@click.option('-d', '--date', type=str, required=True, help="format is %Y%m%d")
@click.option('-n', '--name', type=str, default='validate', help="csv file name")
def click_main(date: str, name: str):
    """
    主函数，解析命令行参数并调用其他函数。
    :param date: 日期
    :param name: CSV 文件名
    """
    date = datetime.strptime(date, '%Y%m%d').date()
    main(date, name)


if __name__ == '__main__':
    click_main()
