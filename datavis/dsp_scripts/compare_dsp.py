# 这个脚本尝试还原产生差异数据的最小过程。
# 结论是发现 s1 left_gaussian 的 wsize 有一个 min 处理，导致了差异。

import pandas as pd
from s1_dsp import smooth_time_axis, smooth_spot_df
from s5_oi import read_file
from compare_conv import compare_df

def dsp_func(df: pd.DataFrame):
    df = smooth_spot_df(df, 15, [200, 1200])
    df = df.reset_index()
    df = df[['dt', 'spot_price_200', 'spot_price_1200']]
    # print(df)
    return df

def main():
    df1 = read_file('159915', 'exp20250528_date20250424',
            wide=False, end_dt='2025-04-24 09:40:00')
    df2 = read_file('159915', 'exp20250528_date20250424',
            wide=False, end_dt='2025-04-24 09:42:00')
    df1 = dsp_func(df1)
    df2 = dsp_func(df2)
    compare_df(df1, df2)

if __name__ == '__main__':
    main()
    
    
