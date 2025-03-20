"""
这里换一个不同的维度 plot 几种交易参数设置的优劣。
我们这里尝试读取所有的 spot all csv 绘制 PNL 曲线。
"""

import glob
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.colors as pc
import plotly.subplots as subplots

from dsp_config import DATA_DIR, gen_wide_suffix

def read_csv(spot: str, suffix: str):
    fs = glob.glob(f'{DATA_DIR}/dsp_stats/{spot}_*{suffix}.csv')
    res = {}
    for fpath in fs:
        df = pd.read_csv(fs)
        # res['']

def plot_stats(df: pd.DataFrame, spot: str):
    pass
