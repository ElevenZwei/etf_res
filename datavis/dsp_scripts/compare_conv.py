# 比较两个 sample conv csv 尝试找出差异

import datetime
import pandas as pd
from dsp_config import DATA_DIR

def cut_df(df: pd.DataFrame, col: str = 'dt', start: str = '09:00:00', end: str = '15:00:00') -> pd.DataFrame:
    df[col] = pd.to_datetime(df[col])
    df = df[(df[col].dt.time >= datetime.time.fromisoformat(start)) & (df[col].dt.time <= datetime.time.fromisoformat(end))]
    df = df.set_index(col)
    return df

def compare_df_col(df1: pd.DataFrame, df2: pd.DataFrame, col: str):
    col1 = df1[col]
    col2 = df2[col]
    col_diff1 = col1[col1 != col2]
    if col_diff1.empty:
        print(f'No difference in column {col}')
    else:
        col_diff2 = col2[col1 != col2]
        col_diff = pd.DataFrame({
            'df1': col_diff1,
            'df2': col_diff2
        })
        print(f'Difference in column {col}:')
        print(col_diff)

def compare_df(df1: pd.DataFrame, df2: pd.DataFrame):
    end = min(df1['dt'].max(), df2['dt'].max()).strftime('%H:%M:%S')
    df1 = cut_df(df1, col='dt', start='09:00:00', end=end)
    df2 = cut_df(df2, col='dt', start='09:00:00', end=end)
    for col in df1.columns:
        if col in df2.columns:
            compare_df_col(df1, df2, col=col)
        else:
            print(f'Column {col} not found in second dataframe')

if __name__ == '__main__':
    # 读取两个 CSV 文件
    df1 = pd.read_csv(f'{DATA_DIR}/compare/merged_159915_sample1.csv')
    df2 = pd.read_csv(f'{DATA_DIR}/compare/merged_159915_sample2.csv')
    compare_df(df1, df2)

# 只有 1200 参数的均值计算会出现差异。

