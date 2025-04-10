"""
Plot residual histogram and scatter plot.
Read both train and validate data.
Make two subplots.
Histogram on the left, scatter plot on the right.
Histogram x axis is residual, y axis is count.
Histogram plot as lines, use different colors for train and validate data.
Scatter plot x axis is real, y axis is residual.
Scatter plot as dots, use different colors for train and validate data.
"""

import os
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import click

def read_df(fpath: str):
    """
    读取 CSV 文件，并返回数据框。
    :param fpath: CSV 文件路径
    :return: 数据框
    """
    df = pd.read_csv(fpath)
    df['dt'] = pd.to_datetime(df['dt'])
    df['dtstr'] = df['dt'].apply(lambda x: x.strftime("%m-%d %H:%M:%S"))
    return df

def read_train_validate_df(dt: datetime.date):
    """
    读取训练集和验证集数据框。
    :param dt: 日期
    :return: 训练集和验证集数据框
    """
    train_df = read_df(f'../output/{dt.strftime("%Y%m%d")}/pred_train.csv')
    validate_df = read_df(f'../output/{dt.strftime("%Y%m%d")}/pred_validate.csv')
    return train_df, validate_df

def plot(train_df, validate_df, dt: datetime.date):
    """
    绘制残差直方图和散点图。
    :param train_df: 训练集数据框
    :param validate_df: 验证集数据框
    """
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Residual Histogram", "Residual Scatter Plot"))

    # Residual Histogram
    fig.add_trace(go.Histogram(x=train_df['residual'], name='Train Residual', opacity=0.75), row=1, col=1)
    fig.add_trace(go.Histogram(x=validate_df['residual'], name='Validate Residual', opacity=0.75), row=1, col=1)

    # Residual Scatter Plot
    fig.add_trace(go.Scatter(x=train_df['real'], y=train_df['residual'], mode='markers', name='Train Residual'), row=1, col=2)
    fig.add_trace(go.Scatter(x=validate_df['real'], y=validate_df['residual'], mode='markers', name='Validate Residual'), row=1, col=2)

    fig.update_layout(title_text=f"Residual Analysis {dt}", showlegend=True)
    fig.update_xaxes(title_text="Residual", row=1, col=1)
    fig.update_yaxes(title_text="Count", row=1, col=1)
    fig.update_xaxes(title_text="Real", row=1, col=2)
    fig.update_yaxes(title_text="Residual", row=1, col=2)

    return fig


def main(dt: datetime.date):
    train_df, validate_df = read_train_validate_df(dt)
    fig = plot(train_df, validate_df, dt)
    fig.show()
    fig.write_image(f"../output/{dt.strftime('%Y%m%d')}/residual_analysis.png")


@click.command()
@click.option('-d', '--date', type=str, required=True, help="format is %Y%m%d")
def click_main(date: str):
    """
    主函数，解析命令行参数并调用其他函数。
    :param date: 日期
    """
    date = datetime.strptime(date, '%Y%m%d').date()
    main(date)

if __name__ == '__main__':
    click_main()
