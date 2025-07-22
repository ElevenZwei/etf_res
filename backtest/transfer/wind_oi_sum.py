import pandas as pd
import datetime
import dateutil.relativedelta as rd
import os

INPUT_DIR = '../data/db/'
OUTPUT_DIR = '../data/oi_output/'

def pivot_sum(df: pd.DataFrame, col: str = 'oi'):
    """
    计算 oi 数据的总和。
    """
    df = df.pivot(index='dt', columns='strike', values=col)
    df = df.ffill().bfill().astype('int64')
    return df.sum(axis=1)

def calc_oi(df: pd.DataFrame):
    df = df.drop_duplicates(subset=['dt', 'code'], keep='first')
    call_df = df[df['callput'] == 1]
    call_sum = pivot_sum(call_df, 'oi')
    put_df = df[df['callput'] == -1]
    put_sum = pivot_sum(put_df, 'oi')
    df2 = pd.DataFrame({
        'call_oi_sum': call_sum,
        'put_oi_sum': put_sum,
    })
    df2['pc'] = df2['put_oi_sum'] - df2['call_oi_sum']
    # spot_price = df[['dt', 'spot_price']].drop_duplicates()
    # spot_price = spot_price.set_index('dt')
    # df2 = pd.merge(df2, spot_price, left_index=True, right_index=True, how='inner')
    return df2.reset_index()

def read_ci(spot: str, dt: datetime.date) -> pd.DataFrame:
    """
    读取合约信息。
    """
    dtstr = dt.strftime('%Y-%m-20')
    fpath = f'{INPUT_DIR}ci_{spot}_{dtstr}.csv'
    if not os.path.exists(fpath):
        print(f"file {fpath} not found")
        raise ValueError(f"File {fpath} not found")
        return pd.DataFrame()
    df = pd.read_csv(fpath)
    df['expirydate'] = pd.to_datetime(df['expirydate'])
    return df

def filter_ci(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤出最近到期的期权合约
    过滤出 tradecode 不含字母 A 的行
    """
    expiry_date = df['expirydate'].min()
    df = df[df['expirydate'] == expiry_date]
    df = df[~df['tradecode'].str.contains('A')]
    return df

def read_opt_md(spot: str, opt: str, dt: datetime.date) -> pd.DataFrame:
    """
    读取期权合约的行情数据。
    """
    dtstr1 = dt.strftime('%Y-%m-01')
    dtstr2 = ((dt.replace(day=1) + rd.relativedelta(months=1, days=-1)).strftime('%Y-%m-%d'))
    fpath = f'{INPUT_DIR}md_{spot}_{opt}_{dtstr1}_{dtstr2}.csv'
    if not os.path.exists(fpath):
        print(f"file {fpath} not found")
        raise ValueError(f"File {fpath} not found")
        return pd.DataFrame()
    df = pd.read_csv(fpath)
    df['dt'] = pd.to_datetime(df['dt'])
    return df

def read_month_opt_md(spot: str, dt: datetime.date) -> pd.DataFrame:
    """
    读取一个月内的所有期权合约行情数据。
    """
    ci_df = read_ci(spot, dt)
    if ci_df.empty:
        return pd.DataFrame()
    
    ci_df = filter_ci(ci_df)
    md_dfs = []
    
    for tup in ci_df.itertuples(index=False):
        opt_code = tup.code
        md_df = read_opt_md(spot, opt_code, dt)
        md_df['callput'] = tup.callput
        md_df['tradecode'] = tup.tradecode
        md_df['strike'] = tup.strike
        md_df['expirydate'] = tup.expirydate
        md_df = md_df.rename(columns={ 'openinterest': 'oi' })
        if not md_df.empty:
            md_dfs.append(md_df)
    
    if not md_dfs:
        return pd.DataFrame()
    
    return pd.concat(md_dfs, ignore_index=True)

def read_calc_oi(spot: str, dt: datetime.date) -> pd.DataFrame:
    md_df = read_month_opt_md(spot, dt)
    if md_df.empty:
        return pd.DataFrame()
    return calc_oi(md_df)

def read_calc_oi_to_csv_year(spot: str, year: int):
    """
    读取并计算期权合约的总持仓量，并保存到 CSV 文件。
    """
    dfs = []
    for month in range(1, 13):
        dt = datetime.date(year, month, 1)
        df = read_calc_oi(spot, dt)
        if df.empty:
            print(f"No data for {spot} on {dt}")
            continue
        dfs.append(df)

    if not dfs:
        print(f"No data for {spot} in {year}")
        return

    combined_df = pd.concat(dfs, ignore_index=True)
    fpath = f'{OUTPUT_DIR}oi_{spot}_{year}.csv'
    combined_df.to_csv(fpath, index=False)
    print(f"Saved OI data to {fpath}")
    return combined_df

read_calc_oi_to_csv_year('159915.SZ', 2024)

# df = read_calc_oi('510500.SH', datetime.date(2024, 12, 1))
# df.to_csv(f'{OUTPUT_DIR}oi_510500.csv', index=False)
