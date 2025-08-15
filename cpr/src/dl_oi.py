"""
这个文件的核心任务是下载一天的 Open Interest 数据。
附带可以把多个 CSV 文件合并成一个 CSV 文件的功能。
"""

from typing import Optional
from collections import deque
import click
import datetime
from dateutil.relativedelta import relativedelta
import io
import os
import sqlalchemy
import pandas as pd

from dsp_config import DATA_DIR, PG_OI_DB_CONF, get_engine

OI_DIR = '{DATA_DIR}/fact/oi_daily/'
if not os.path.exists(OI_DIR):
    os.makedirs(OI_DIR, exist_ok=True)

engine = get_engine(PG_OI_DB_CONF)


def read_last_row(csv_path: str, encoding: str = "utf-8") -> pd.Series:
    """
    高效读取 CSV 文件的最后一行（包括 header 解析）。
    适合大文件。
    """
    with open(csv_path, "r", encoding=encoding) as f:
        header = next(f)                          # 第一行是列名
        last_line = deque(f, maxlen=1)[0]         # 只保留最后一行
    csv_fragment = header + last_line
    df = pd.read_csv(io.StringIO(csv_fragment))
    return df.iloc[0]  # 返回 Series 类型的一行


def dl_expiry_date(spot: str, year: int, month: int) -> Optional[datetime.date]:
    d_from = datetime.datetime(year, month, 1)
    d_to = datetime.datetime(year, month, 28)
    query = f"""
        select min(expirydate) as expirydate
        from contract_info ci
        where spotcode like '{spot}%%'
        and expirydate >= '{d_from.strftime('%Y-%m-%d')}'
        and expirydate <= '{d_to.strftime('%Y-%m-%d')}'
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    if df.shape[0] == 0:
        return None
    return df['expirydate'].iloc[0]


def dl_oi_data(spot: str, expiry_date: datetime.date,
        bg_date: datetime.date, ed_date: datetime.date,
        bg_time: datetime.time = datetime.time(9, 30, 0),
        ed_time: datetime.time = datetime.time(15, 0, 0),) -> pd.DataFrame:
    expiry_date_str = expiry_date.strftime('%Y-%m-%d')
    bg_date_str = bg_date.strftime('%Y-%m-%d')
    ed_date_str = ed_date.strftime('%Y-%m-%d')
    bg_time_str = bg_time.strftime('%H:%M:%S')
    ed_time_str = ed_time.strftime('%H:%M:%S')
    query = f"""
        set enable_nestloop=false;
        with OI as (
            select *
            from (
                select
                    dt, spotcode, expirydate, callput, strike, tradecode,
                    open_interest as oi
                from market_data_tick mdt join contract_info ci using(code)
                where dt > '{bg_date_str} {bg_time_str}' and dt < '{ed_date_str} {ed_time_str}'
                and dt::time >= '09:30:00' and dt::time <= '15:00:00'
                and spotcode = '{spot}' and expirydate = '{expiry_date_str}'
            ) as T
        )
        select oi.dt, oi.spotcode, oi.expirydate, oi.strike
            , oi.callput, oi.tradecode, oi.oi, mdt.last_price as spot_price
        from OI join market_data_tick mdt using (dt)
        where dt > '{bg_date_str} {bg_time_str}' and dt < '{ed_date_str} {ed_time_str}'
        and mdt.code = oi.spotcode
        order by dt asc;
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    if df.shape[0] == 0:
        return df
    df['dt'] = df['dt'].dt.tz_convert('Asia/Shanghai')
    return df


def save_fpath(spot: str, tag: str,
        bg_date: datetime.date,
        ed_date: datetime.date,
        expiry_date: datetime.date):
    bg_date_str = bg_date.strftime('%Y%m%d')
    ed_date_str = ed_date.strftime('%Y%m%d')
    date_suffix = bg_date_str if bg_date == ed_date else f'{bg_date_str}_{ed_date_str}'
    expiry_date_str = expiry_date.strftime('%Y%m%d')
    suffix = f'exp{expiry_date_str}_date{date_suffix}'
    fname = f'strike_oi_{tag}_{spot}_{suffix}.csv'
    fpath = f'{OI_DIR}/{fname}'
    return fpath


def dl_save_range_oi(spot: str, expiry_date: datetime.date,
        bg_date: datetime.date, ed_date: datetime.date,
        ):
    fpath = save_fpath(spot, 'raw', bg_date, ed_date, expiry_date)
    bg_time = datetime.time(9, 30, 0)
    df1 = pd.DataFrame()
    # 如果文件存在，读取最后一行的时间戳
    if os.path.exists(fpath):
        df1 = pd.read_csv(fpath)
        # check format
        if 'tradecode' in df1.columns:
            last_row = df1.iloc[-1]
            last_dt = pd.to_datetime(last_row['dt'])
            bg_date = last_dt.date()
            bg_time = (last_dt + datetime.timedelta(seconds=1)).time()
        if bg_time > datetime.time(14, 59, 0):
            return df1
    df2 = dl_oi_data(spot, expiry_date,
            bg_date, ed_date,
            bg_time=bg_time,
            ed_time=datetime.time(15, 0, 0))
    dfs = [x for x in [df1, df2] if x.shape[0] != 0]
    if dfs == []:
        raise RuntimeError("db is empty.")
    df = pd.concat(dfs, ignore_index=True)
    if df.shape[0] == 0:
        raise RuntimeError("db is empty.")
    df.to_csv(fpath, index=False)
    return df


def get_nearest_expirydate(spot: str, dt: datetime.date):
    exp: Optional[datetime.date] = dl_expiry_date(spot, dt.year, dt.month)
    if exp is None:
        return exp
    # 对于当月交割日已经过去的情况，切换到下一个月
    if exp < dt:
        dt += relativedelta(months=1)
        exp = dl_expiry_date(spot, dt.year, dt.month)
    return exp


def dl_raw_daily(spot: str, dt: datetime.date):
    expiry_date = get_nearest_expirydate(spot, dt)
    if expiry_date is None:
        raise RuntimeError("cannot find expiry date.")
    return dl_save_range_oi(spot, expiry_date, dt, dt)


def save_fpath_default(spot: str, tag: str, dt: datetime.date):
    expiry_date = get_nearest_expirydate(spot, dt)
    if expiry_date is None:
        raise RuntimeError("cannot find expiry date.")
    return save_fpath(spot, tag, dt, dt, expiry_date)


def pivot_sum(df: pd.DataFrame, col: str = 'oi'):
    """
    计算 oi 数据的总和。
    """
    df = df.pivot(index='dt', columns='strike', values=col)
    df = df.ffill().bfill().astype('int64')
    return df.sum(axis=1)


def calc_oi(df: pd.DataFrame):
    df = df.drop_duplicates(subset=['dt', 'tradecode'], keep='first')
    call_df = df[df['callput'] == 1]
    call_sum = pivot_sum(call_df, 'oi')
    put_df = df[df['callput'] == -1]
    put_sum = pivot_sum(put_df, 'oi')
    df2 = pd.DataFrame({
        'call_oi_sum': call_sum,
        'put_oi_sum': put_sum,
    })
    spot_price = df[['dt', 'spot_price']].drop_duplicates()
    spot_price = spot_price.set_index('dt')
    df2 = pd.merge(df2, spot_price,
                   left_index=True,
                   right_index=True,
                   how='inner')
    return df2.reset_index()


def dl_calc_oi(spot: str, dt: datetime.date, refresh: bool = False):
    """
    计算 oi 数据。
    """
    if refresh:
        # 如果需要刷新，先下载原始数据
        df = dl_raw_daily(spot, dt)
    else:
        # 如果不需要刷新，直接读取原始数据
        fpath = save_fpath_default(spot, 'raw', dt)
        if not os.path.exists(fpath):
            print(f"File {fpath} does not exist, downloading...")
            df = dl_raw_daily(spot, dt)
        else:
            df = pd.read_csv(fpath)
    # 计算 oi 数据
    df = calc_oi(df)
    # 保存结果
    fpath_oi = save_fpath_default(spot, 'oi', dt)
    df.to_csv(fpath_oi, index=False)
    return df


def dl_calc_oi_range(spot: str, bg_date: datetime.date, ed_date: datetime.date):
    """
    下载并计算一段时间内的 oi 数据。
    """
    dt_list = pd.date_range(bg_date, ed_date).to_list()
    for dt in dt_list:
        dt = dt.date()
        try:
            dl_calc_oi(spot, dt, refresh=True)
        except Exception as e:
            print(f"Error processing {spot} on {dt}: {e}")

