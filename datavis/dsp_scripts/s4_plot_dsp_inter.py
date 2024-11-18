"""
对于期权 OI 很多曲面的交叉线做一个二维绘图。
"""

import click
import datetime
import plotly.graph_objects as go
import plotly.colors as pc
import pandas as pd

def standard_prices(se: pd.Series):
    # return (se - se.mean()) / se.std()
    return (se - se.iloc[0]) / (se.iloc[0] * 0.01)

# OI 使用 / std 和 / 1000 在曲线之间的关系上会产生区别。
def standard_oi_diff(se: pd.Series):
    return (se - se.iloc[0]) / 500
    # return (se - se.mean()) / se.std()

def plot_df(df: pd.DataFrame, title: str):
    print(df.columns)
    df['dt'] = pd.to_datetime(df['dt'])

    # clip morning.
    # df = df.loc[df['dt'].dt.time < datetime.time(13)].copy()

    df.loc[:, 'sdt'] = df['dt'].apply(lambda x: x.strftime('%m-%d %H:%M:%S'))
    df = df.drop(columns=['dt'])
    df['dt'] = df['sdt']
    x_uni = df['dt']
    y_spot = standard_prices(df['spot_price'])
    y_spot_300 = standard_prices(df['spot_price_300'])

    y_pc_120_3 = -1 * standard_oi_diff(df['oi_cp_120_0.3'])
    y_pc_300_3 = -1 * standard_oi_diff(df['oi_cp_300_0.3'])
    y_pc_600_3 = -1 * standard_oi_diff(df['oi_cp_600_0.3'])
    y_pc_1200_3 = -1 * standard_oi_diff(df['oi_cp_1200_0.3'])

    y_pc_120_4 = -1 * standard_oi_diff(df['oi_cp_120_0.4'])
    y_pc_300_4 = -1 * standard_oi_diff(df['oi_cp_300_0.4'])
    y_pc_600_4 = -1 * standard_oi_diff(df['oi_cp_600_0.4'])
    y_pc_1200_4 = -1 * standard_oi_diff(df['oi_cp_1200_0.4'])

    y_pc_120_5 = -1 * standard_oi_diff(df['oi_cp_120_0.5'])
    y_pc_300_5 = -1 * standard_oi_diff(df['oi_cp_300_0.5'])
    y_pc_600_5 = -1 * standard_oi_diff(df['oi_cp_600_0.5'])
    y_pc_1200_5 = -1 * standard_oi_diff(df['oi_cp_1200_0.5'])

    y_pc_120_6 = -1 * standard_oi_diff(df['oi_cp_120_0.6'])
    y_pc_300_6 = -1 * standard_oi_diff(df['oi_cp_300_0.6'])
    y_pc_600_6 = -1 * standard_oi_diff(df['oi_cp_600_0.6'])
    y_pc_1200_6 = -1 * standard_oi_diff(df['oi_cp_1200_0.6'])

    y_pc_120_8 = -1 * standard_oi_diff(df['oi_cp_120_0.8'])
    y_pc_300_8 = -1 * standard_oi_diff(df['oi_cp_300_0.8'])
    y_pc_600_8 = -1 * standard_oi_diff(df['oi_cp_600_0.8'])
    y_pc_1200_8 = -1 * standard_oi_diff(df['oi_cp_1200_0.8'])

    line_plot = go.Figure()
    
    cs = pc.sequential.tempo

    line_plot.add_trace(go.Scatter(x=x_uni, y=y_spot, mode='lines', name='spot', line={'color': cs[1]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_spot_300, mode='lines', name='spot 300', line={'color': cs[4]}))
    
    c3 = pc.sequential.Peach
    c4 = pc.sequential.Burg
    c5 = pc.sequential.Magenta
    c6 = pc.sequential.Purp
    c8 = pc.sequential.Teal

    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_120_3, mode='lines', name='pc 120 0.3', line={'color': c3[1]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_300_3, mode='lines', name='pc 300 0.3', line={'color': c3[2]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_600_3, mode='lines', name='pc 600 0.3', line={'color': c3[3]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_1200_3, mode='lines', name='pc 1200 0.3', line={'color': c3[4]}))

    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_120_4, mode='lines', name='pc 120 0.4', line={'color': c4[1]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_300_4, mode='lines', name='pc 300 0.4', line={'color': c4[2]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_600_4, mode='lines', name='pc 600 0.4', line={'color': c4[3]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_1200_4, mode='lines', name='pc 1200 0.4', line={'color': c4[4]}))

    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_120_5, mode='lines', name='pc 120 0.5', line={'color': c5[1]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_300_5, mode='lines', name='pc 300 0.5', line={'color': c5[2]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_600_5, mode='lines', name='pc 600 0.5', line={'color': c5[3]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_1200_5, mode='lines', name='pc 1200 0.5', line={'color': c5[4]}))

    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_120_6, mode='lines', name='pc 120 0.6', line={'color': c6[1]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_300_6, mode='lines', name='pc 300 0.6', line={'color': c6[2]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_600_6, mode='lines', name='pc 600 0.6', line={'color': c6[3]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_1200_6, mode='lines', name='pc 1200 0.6', line={'color': c6[4]}))

    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_120_8, mode='lines', name='pc 120 0.8', line={'color': c8[1]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_300_8, mode='lines', name='pc 300 0.8', line={'color': c8[2]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_600_8, mode='lines', name='pc 600 0.8', line={'color': c8[3]}))
    line_plot.add_trace(go.Scatter(x=x_uni, y=y_pc_1200_8, mode='lines', name='pc 1200 0.8', line={'color': c8[4]}))

    line_plot.update_layout(
        title=f'{title} OI Intersects',
        xaxis_title='Time',
        yaxis_title='OI',
    )
    line_plot.show()

def plot_file(spot: str, suffix: str):
    df = pd.read_csv(f'../dsp_conv/merged_{spot}_{suffix}.csv')
    plot_df(df, title=f"{spot} {suffix}")

def main(spot: str, suffix: str):
    plot_file(spot, suffix)

@click.command()
@click.option('-s', '--spot', type=str, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, help="csv file name suffix.")
def click_main(spot: str, suffix: str):
    main(spot, suffix)

if __name__ == '__main__':
    # plot_file('159915', '20241104')
    # plot_file('159915', '20241108')
    # plot_file('159915', '20241105')
    # plot_file('159915', '20241106')
    # plot_file('159915', '20241112')
    # plot_file('510050', '20241114_am')
    click_main()
    pass
