# 通联数据的 price 指 openp，所以需要把时间向前移动一点才正确

import pandas as pd
from datetime import timedelta

def shift_dt(df: pd.DataFrame):
    df['dt'] = pd.to_datetime(df['dt'])
    df['dt'] = df['dt'] - timedelta(minutes=1)
    return df

# df = pd.read_csv('input/tl_greeks_159915_clip.csv')
df = pd.read_csv('input/tl_greeks_159915_all.csv')
shift_dt(df)
# df.to_csv('input/tl_greeks_159915_clip_fixed.csv')
df.to_csv('input/tl_greeks_159915_all_fixed.csv')
