"""
从数据集中生成中间数据。
读取原始的股指，期权价格，然后生成线性回归使用的数据集。
需要生成这样一些数据列：
1. 期权价格
2. 期权价格的日内移动，用百分比表示
3. 股指的日内移动，用百分比表示
4. 股指的日内移动方差
5. 期权价格的30日移动方差
"""
import pandas as pd
import numpy as np
import os

RAW_INPUT_DIR = '../input/原始期权和期货1min连续数据'
OUTPUT_DIR = '../input/生成中间数据'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def calc_30days_var(df: pd.DataFrame, col: str, days: int) -> pd.DataFrame:
    """
    对于每一个日期，划分出它之前30天的所有数据，计算这30天的方差
    """
    dates = df['date'].drop_duplicates()
    res = dates.to_frame()
    for date in dates:
        # 计算每个日期之前30天的方差
        mask = (df['date'] < date) & (df['date'] >= date - pd.Timedelta(days=days))
        temp_df = df[mask]
        if temp_df.shape[0] > 0:
            var = temp_df[col].var()
            res.loc[res['date'] == date, 'var'] = var
    res['var'] = res['var'].fillna(0)  # 填充NaN为0
    return res

def calc_daily_movement_var(df: pd.DataFrame, df_melt: pd.DataFrame, col: str) -> pd.DataFrame:
    df = df.sort_values(['date', 'dt'])
    df = df.set_index('dt')
    df['open_price'] = df.groupby('date')[col].transform('first')
    df['daily_movement'] = df[col] / df['open_price'] - 1  # 日内移动

    # 我需要计算的是到当前这一行为止的日内移动方差，应该用一个窗口函数完成
    df_melt = df_melt.sort_values(['date', 'dt'])
    df_melt['daily_movement_var'] = df_melt.groupby('date')[col].expanding().var().reset_index(level=0, drop=True) # 标准差
    df_melt['daily_movement_var'] = df_melt['daily_movement_var'].fillna(0)  # 填充NaN为0
    # select the last row of each dt
    df_melt = df_melt.groupby(['dt']).last()
    # print(df_melt)
    df['daily_movement_var'] = df_melt['daily_movement_var']
    df = df.reset_index()
    return df

def read_opt(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    df['dt'] = pd.to_datetime(df['date'])
    # check if NaT
    nat_lines = df[df['dt'].isnull()]
    if nat_lines.shape[0] > 0:
        print(f"NaT lines in {file_path}:")
        print(nat_lines)
    df = df[df['dt'].notnull()]
    df = df[(df['dt'].dt.hour != 15)]
    df = df[~((df['dt'].dt.hour == 11) & (df['dt'].dt.minute == 30))]

    df['date'] = df['dt'].dt.date
    df_melt = df.melt(id_vars=['dt', 'date'], 
                      value_vars=['high', 'low', 'close'],
                      var_name='type', value_name='price')

    df['price'] = df['close'].astype(float)
    df = df[['dt', 'date', 'price']]
    df = calc_daily_movement_var(df, df_melt, 'price')
    df = df.rename(columns={
            'price': 'option-price',
            'daily_movement': 'option-ret',
            'daily_movement_var': 'option-daily-ret-var'})
    df2 = calc_30days_var(df_melt, 'price', 7)
    df2 = df2.rename(columns={'var': 'option-ret-var'})
    df = df.merge(df2, on='date', how='left')
    # df = df[['dt', 'option-price', 'option-ret', 'option-ret-var']]
    df['option-ret-var'] = df['option-ret-var'] * 10000
    df = df[['dt', 'option-price', 'option-ret', 'option-ret-var']]
    return df


def read_stock_index(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path, header=None, encoding='gbk',
            dtype={0: str, 1: str, 2: float, 3: float, 4: float, 5: float, 6: float, 7: float})
    df.columns = ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'open_interest']
    # print(df)
    df['dt'] = pd.to_datetime(df['date'] + ' ' + df['time'])
    df = df[(df['dt'].dt.hour > 9)
            | ((df['dt'].dt.hour == 9) & (df['dt'].dt.minute >= 30))]
    df['date'] = df['dt'].dt.date
    # df = df[:1000]
    df_melt = df.melt(id_vars=['dt', 'date'], 
                      value_vars=['high', 'low', 'close'],
                      var_name='type', value_name='price')

    df['price'] = df['close'].astype(float)
    df = df[['dt', 'date', 'price']]
    df = calc_daily_movement_var(df, df_melt, 'price')
    df = df.rename(columns={
            'price': 'index-price',
            'daily_movement': 'if-ret',
            'daily_movement_var': 'if-ret-var'})
    df = df[['dt', 'index-price', 'if-ret', 'if-ret-var']]
    return df

# df = read_opt(f'{RAW_INPUT_DIR}/实1看涨分钟连续.csv')
# df = read_opt(f'{RAW_INPUT_DIR}/虚1看涨分钟连续.csv')
# df = read_opt(f'{RAW_INPUT_DIR}/虚1看跌分钟连续.csv')
# df = read_stock_index(f'{RAW_INPUT_DIR}/IF股指.csv')
# print(df)

def main():
    df_opt = read_opt(f'{RAW_INPUT_DIR}/实1看涨分钟连续.csv')
    df_index = read_stock_index(f'{RAW_INPUT_DIR}/IF股指.csv')
    df_index.to_csv(f'{OUTPUT_DIR}/股指数据.csv', index=False)
    df = df_opt.merge(df_index, on='dt', how='left')
    df = df.rename(columns={'dt': 'date'})
    df.to_csv(f'{OUTPUT_DIR}/中间数据.csv', index=False)

if __name__ == '__main__':
    main()

