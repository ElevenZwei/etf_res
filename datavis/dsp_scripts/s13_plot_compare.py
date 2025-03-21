"""
这个脚本用曲线图绘制 s12 输出的 csv 。
"""

import click
import pandas as pd
import plotly.graph_objects as go
import plotly.colors as pc
import plotly.subplots as subplots

from dsp_config import DATA_DIR, gen_wide_suffix

PLOT_CONFIG = {
    'fixed': {
        'toss': pc.sequential.Purp,
        'tosr': pc.sequential.Peach,
        'totp': pc.sequential.Teal,

        'ts': pc.sequential.Emrld[::-1],
        'sigma': pc.sequential.Agsunset[1:],
        'ts_sigma': pc.sequential.Agsunset[2:]
    },
    'color_seqs': [
        pc.sequential.Purp,
        pc.sequential.Purpor,
    ]
}

class ColorMapper:
    def __init__(self, desc: list[str]):
        # remove digit suffix for every string in desc
        self.desc_st_map = {d: d.rstrip('0123456789') for d in desc}
        self.st = list(set(self.desc_st_map.values()))
        self.st_seq_map = {}
        for i, c in enumerate(self.st):
            if c in PLOT_CONFIG['fixed']:
                self.st_seq_map[c] = PLOT_CONFIG['fixed'][c]
            else:
                self.st_seq_map[c] = PLOT_CONFIG['color_seqs'][i % len(PLOT_CONFIG['color_seqs'])]
        self.desc_color_map = {}
        for d in desc:
            suffix = d[len(self.desc_st_map[d]):]
            desc_seq = self.st_seq_map[self.desc_st_map[d]]
            self.desc_color_map[d] = desc_seq[int(suffix)]
    
    def get_color(self, desc: str):
        return self.desc_color_map[desc]

def plot_cols(df: pd.DataFrame, x_col, y_prefix, y_name, color_mapper, fig, row, col):
    cols = [col for col in df.columns if y_prefix in col]
    for c in cols:
        arg_desc = c.split('@')[1]
        fig.add_trace(go.Scatter(
                x=df[x_col], y=df[c].ffill(), mode='lines', name=arg_desc,
                line=dict(color=color_mapper.get_color(arg_desc))
            ), row=row, col=col)
    # set x and y axis titles
    fig.update_xaxes(title_text="Date", row=row, col=col)
    fig.update_yaxes(title_text=y_name, row=row, col=col)
    return fig

def plot(df: pd.DataFrame, spot: str, suffix: str, show: bool, save: bool):
    df['date'] = pd.to_datetime(df['date'])
    hold_time_cols = [col for col in df.columns if 'hold_time' in col]
    for col in hold_time_cols:
        df[col] = pd.to_timedelta(df[col])
    cols = [col for col in df.columns if '@' in col]
    desc = [col.split('@')[1] for col in cols]
    # remove duplicates in desc
    desc = list(set(desc))
    desc.sort()
    print(f"Comparing {len(desc)} strategies on {spot}: {', '.join(desc)}")
    color_mapper = ColorMapper(desc)

    fig = subplots.make_subplots(rows=1, cols=2,
            subplot_titles=(
                # "PNL Accumulated",
                "PNL Percent Accumulated",
                "Hold Time Accumulated",
            ))
    # fig = plot_cols(df, 'date', 'pnl_acc@', 'PNL Accumulated', color_mapper, fig, 1, 1)
    fig = plot_cols(df, 'date', 'pnl_p_acc@', 'PNL Percent Accumulated', color_mapper, fig, 1, 1)
    fig = plot_cols(df, 'date', 'hold_time_acc@', 'Hold Time Accumulated', color_mapper, fig, 1, 2)
    fig.update_layout(title_text=f'PNL Comparison for {spot} {suffix}')
    if save:
        fig.write_image(f"{DATA_DIR}/dsp_plot/pnl_acc_compare_{spot}_{suffix}.png")
    if show:
        fig.show()
    

def main(spot: str, suffix: str, show: bool, save: bool):
    # 读取所有的 csv 文件
    file_path = f"{DATA_DIR}/dsp_stats/{spot}_compare_rollup_{suffix}.csv"
    df = pd.read_csv(file_path)
    plot(df, spot, suffix, show, save)

@click.command()
@click.option('-s', '--spot', required=True, help='Spot name to compare')
@click.option('-d', '--suffix', required=True, help='Suffix for the file name')
@click.option('--show', type=bool, default=True, help='Show the plot')
@click.option('--save', type=bool, default=True, help='Save the plot as a PNG file')
def cli(spot, suffix, show, save):
    main(spot, suffix, show, save)

if __name__ == "__main__":
    cli()
    
