"""
这个文件的核心任务是下载一天的 Open Interest 数据。
附带可以把多个 CSV 文件合并成一个 CSV 文件的功能。
"""

import click
import datetime
import glob
import os
import sqlalchemy as sa
import pandas as pd
from typing import Optional, List
from dateutil.relativedelta import relativedelta
from concurrent.futures import ProcessPoolExecutor, as_completed

from config import DATA_DIR, PG_CPR_CONN_INFO, PG_OI_CONN_INFO, get_engine

OI_DIR = f'{DATA_DIR}/fact/oi_daily/'
OI_MERGE_DIR = f'{DATA_DIR}/fact/oi_merge/'
USE_NEW_DB = True

if not os.path.exists(OI_DIR):
    os.makedirs(OI_DIR, exist_ok=True)
if not os.path.exists(OI_MERGE_DIR):
    os.makedirs(OI_MERGE_DIR, exist_ok=True)

def get_engine_wrapper() -> sa.engine.Engine:
    return get_engine(PG_CPR_CONN_INFO if USE_NEW_DB else PG_OI_CONN_INFO)

engine: sa.engine.Engine = get_engine_wrapper()


def dl_expiry_date(spot: str, year: int, month: int) -> Optional[datetime.date]:
    d_from = datetime.datetime(year, month, 1)
    d_to = datetime.datetime(year, month, 28)
    if USE_NEW_DB:
        return fetch_expiry_date_new(spot, d_from.date(), d_to.date())
    else:
        return fetch_expiry_date_old(spot, d_from.date(), d_to.date())


def fetch_expiry_date_old(spot: str, d_from: datetime.date, d_to: datetime.date) -> Optional[datetime.date]:
    # latex: ci.expirydate \in [d_from, d_to]
    query = sa.text("""
            select min(expirydate) as expirydate
            from contract_info ci
            where spotcode like :spot_code
            and expirydate >= :d_from
            and expirydate <= :d_to
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'spot_code': f'{spot}%',
            'd_from': d_from.strftime('%Y-%m-%d'),
            'd_to': d_to.strftime('%Y-%m-%d'),
        })
    if df.shape[0] == 0:
        return None
    return df.iloc[0, 0]


def fetch_expiry_date_new(spot: str, d_from: datetime.date, d_to: datetime.date) -> Optional[datetime.date]:
    query = sa.text("""
                    select min(expiry)
                    from "md"."contract_info"
                    where expiry >= :d_from and expiry <= :d_to
                    and spotcode = :spot
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'spot': spot,
            'd_from': d_from.strftime('%Y-%m-%d'),
            'd_to': d_to.strftime('%Y-%m-%d'),
        })
    if df.shape[0] == 0:
        return None
    return df.iloc[0, 0]


def dl_oi_data(spot: str, expiry_date: datetime.date,
        bg_date: datetime.date, ed_date: datetime.date,
        bg_time: datetime.time = datetime.time(9, 30, 0),
        ed_time: datetime.time = datetime.time(15, 0, 0)) -> pd.DataFrame:
    """
    bg_date and ed_date are inclusive.
    bg_time and ed_time are inclusive.
    """
    bg_datetime_str = bg_date.strftime('%Y-%m-%d') + ' ' + bg_time.strftime('%H:%M:%S')
    ed_datetime_str = ed_date.strftime('%Y-%m-%d') + ' ' + ed_time.strftime('%H:%M:%S')
    if USE_NEW_DB:
        fetch_oi_data = fetch_oi_data_new(spot, expiry_date, bg_datetime_str, ed_datetime_str)
    else:
        fetch_oi_data = fetch_oi_data_old(spot, expiry_date, bg_datetime_str, ed_datetime_str)
    return fetch_oi_data


def fetch_oi_data_old(spot: str, expiry_date: datetime.date,
                      bg_datetime_str: str, ed_datetime_str: str) -> pd.DataFrame:
    # latex: mdt.dt \in [bg_datetime, ed_datetime]
    query = sa.text("""
        set enable_nestloop=false;
        with OI as (
            select *
            from (
                select
                    dt, spotcode, expirydate, callput, strike, tradecode,
                    open_interest as oi
                from market_data_tick mdt join contract_info ci using(code)
                where mdt.dt >= :bg_datetime and mdt.dt <= :ed_datetime
                and dt::time >= '09:30:00' and dt::time <= '15:00:00'
                and spotcode = :spot and expirydate = :expiry_date
            ) as T
        )
        select * from OI order by dt asc;
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'spot': spot,
            'expiry_date': expiry_date.strftime('%Y-%m-%d'),
            'bg_datetime': bg_datetime_str,
            'ed_datetime': ed_datetime_str,
        })
    return df


def fetch_oi_data_new(spot: str, expiry_date: datetime.date,
                      bg_datetime_str: str, ed_datetime_str: str) -> pd.DataFrame:
    query = sa.text("""
        with tradecodes as (
            select spotcode, expiry, callput, strike, tradecode
            from "md"."contract_info"
            where expiry = :expiry_date and spotcode = :spot
        )
        , oi as (
            select dt, spotcode, expiry, callput, strike, tradecode, oi
            from md.contract_price_tick join tradecodes using (tradecode)
            where dt >= :bg_datetime and dt < :ed_datetime
        )
        select * from oi order by dt asc;
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'spot': spot,
            'expiry_date': expiry_date.strftime('%Y-%m-%d'),
            'bg_datetime': bg_datetime_str,
            'ed_datetime': ed_datetime_str,
        })
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
    fname = f'{tag}_{spot}_{suffix}.csv'
    fpath = f'{OI_DIR}/{fname}'
    return fpath


def get_cont_time_from_df(df1: pd.DataFrame) -> datetime.time:
    bg_time = datetime.time(9, 30, 0)
    # 开头不完善或者格式不正确那么就从头开始下载
    first_row = df1.iloc[0] if not df1.empty else None
    if first_row is not None:
        if 'tradecode' not in first_row:
            return bg_time
        last_dt = pd.to_datetime(first_row['dt'])
        if last_dt.time() > datetime.time(9, 31, 0):
            return bg_time
    last_row = df1.iloc[-1] if not df1.empty else None
    if last_row is not None and 'tradecode' in last_row:
        last_dt = pd.to_datetime(last_row['dt'])
        bg_time = (last_dt + datetime.timedelta(seconds=1)).time()
    return bg_time


def dl_save_range_oi(spot: str, expiry_date: datetime.date,
        bg_date: datetime.date):
    ed_date = bg_date
    fpath = save_fpath(spot, 'raw', bg_date, ed_date, expiry_date)
    bg_time = datetime.time(9, 30, 0)
    df1 = None
    # 如果文件存在，读取最后一行的时间戳
    if os.path.exists(fpath):
        df1 = pd.read_csv(fpath, parse_dates=['dt'])
        bg_time = get_cont_time_from_df(df1)
        if bg_time >= datetime.time(14, 59, 0):
            print(f"Loading existing data for {spot} on {bg_date}, "
                  f"expiry date: {expiry_date}")
            return pd.read_csv(fpath)
    print(f"Downloading raw data for {spot} on {bg_date}"
          f", expiry date: {expiry_date}")
    df2 = dl_oi_data(spot, expiry_date,
            bg_date, ed_date,
            bg_time=bg_time,
            ed_time=datetime.time(15, 0, 0))
    if df1 is not None and not df1.empty:
        df1['dt'] = df1['dt'].dt.tz_convert('Asia/Shanghai')
    if df2 is not None and not df2.empty:
        df2['dt'] = df2['dt'].dt.tz_convert('Asia/Shanghai')
    if bg_time > datetime.time(9, 30, 0):
        dfs = [x for x in [df1, df2] if x is not None and x.shape[0] != 0]
    else:
        dfs = [df2] if df2 is not None and df2.shape[0] != 0 else []
    if dfs == []:
        raise RuntimeError("db is empty.")
    df = pd.concat(dfs, ignore_index=True)
    if df.shape[0] == 0:
        raise RuntimeError("db is empty.")
    # 手动将时间戳转换为字符串格式，因为自动转换有的带微秒，有的微秒恰好是 0 ，格式里面会有差异。
    df['dt'] = df['dt'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')
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
    return dl_save_range_oi(spot, expiry_date, dt)


def save_fpath_default(spot: str, tag: str, dt: datetime.date):
    expiry_date = get_nearest_expirydate(spot, dt)
    if expiry_date is None:
        raise RuntimeError("cannot find expiry date.")
    return save_fpath(spot, tag, dt, dt, expiry_date)


def pivot_sum(df: pd.DataFrame, col: str, tag: str):
    """
    计算 oi 数据的总和。
    """
    df = df.pivot(index='dt', columns='strike', values=col)
    df = df.ffill().bfill().astype('int64')
    # df.to_csv(f'{OI_DIR}/debug_pivot_sum_{tag}.csv')
    return df.sum(axis=1)


def calc_oi(df: pd.DataFrame):
    df = df.drop_duplicates(subset=['dt', 'tradecode'], keep='first')
    call_df = df.loc[df['callput'] == 1]
    call_sum = pivot_sum(call_df, 'oi', 'call')
    put_df = df.loc[df['callput'] == -1]
    put_sum = pivot_sum(put_df, 'oi', 'put')
    df2 = pd.DataFrame({
        'call_oi_sum': call_sum,
        'put_oi_sum': put_sum,
    })
    return df2.reset_index()
    # spot_price = df[['dt', 'spot_price']].drop_duplicates()
    # spot_price = spot_price.set_index('dt')
    # df2 = pd.merge(df2, spot_price,
    #                left_index=True,
    #                right_index=True,
    #                how='inner')
    # return df2.reset_index()


def dl_calc_oi(spot: str, dt: datetime.date, refresh: bool = False) -> pd.DataFrame:
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


def date_range(bg_date: datetime.date, ed_date: datetime.date) -> List[datetime.date]:
    dt_list: List[pd.Timestamp] = pd.date_range(bg_date, ed_date).to_list()
    holidays = [
        '2025-01-28', '2025-01-29', '2025-01-30', '2025-01-31',
        '2025-02-01', '2025-02-02', '2025-02-03', '2025-02-04',
        '2025-04-04',
        '2025-05-01', '2025-05-02', '2025-05-03', '2025-05-04', '2025-05-05',
        '2025-06-02',
        '2025-10-01', '2025-10-02', '2025-10-03',
        '2025-10-06', '2025-10-07', '2025-10-08',
    ]
    dt_list = [dt for dt in dt_list if dt.weekday() < 5]  # skip weekend
    dt_list = [dt for dt in dt_list if dt.strftime('%Y-%m-%d') not in holidays]  # skip holidays
    date_list = [dt.date() for dt in dt_list]  # convert to date
    return date_list


def init_worker():
    """
    初始化工作进程，设置数据库连接等。
    """
    global engine
    engine = get_engine_wrapper()


def dl_calc_oi_range(
        spot: str, bg_date: datetime.date, ed_date: datetime.date) -> pd.DataFrame:
    """
    下载并计算一段时间内的 oi 数据。
    """
    dt_list = date_range(bg_date, ed_date)
    df_list = []
    # for dt in dt_list:
    #     dt = dt.date()
    #     try:
    #         df = dl_calc_oi(spot, dt, refresh=True)
    #         print(f"Successfully processed {spot} on {dt}")
    #         df_list.append(df)
    #     except Exception as e:
    #         print(f"Error processing {spot} on {dt}: {e}")
    #       # raise e
    with ProcessPoolExecutor(initializer=init_worker, max_workers=10) as executor:
        futures = {executor.submit(dl_calc_oi, spot, dt, True): dt
                   for dt in dt_list}
        for future in as_completed(futures):
            dt = futures[future]
            try:
                df = future.result()  # 获取结果，可能会抛出异常
                print(f"Successfully got oi {spot} on {dt}")
                print(df.head())
                print(df.tail())
                df_list.append(df)
            except Exception as e:
                print(f"Error getting oi {spot} on {dt}: {e}")
                raise e
    if df_list == []:
        raise RuntimeError("No data downloaded.")
    df = pd.concat(df_list, ignore_index=True)
    return df


def oi_csv_merge(spot: str):
    fs = glob.glob(f"{OI_DIR}/oi_{spot}*.csv")
    dfs = [pd.read_csv(f) for f in fs]
    df = pd.concat(dfs, ignore_index=True)
    print(df.head())
    df['dt'] = pd.to_datetime(df['dt'], utc=True)
    df = df.sort_values(by=['dt'])
    df = df.drop_duplicates(subset=['dt'], keep='first')
    df.to_csv(f"{OI_MERGE_DIR}/oi_{spot}.csv", index=False)
    print(f"merged {len(fs)} files into {OI_MERGE_DIR}/oi_{spot}.csv")
    return df


@click.command()
@click.option('-s', '--spot', type=str, required=True, help="Spot code: e.g., 159915 510050")
@click.option('-b', '--begin', type=str, help="Begin date in format YYYYMMDD")
@click.option('-e', '--end', type=str, help="End date in format YYYYMMDD")
def click_main(spot: str, begin: str, end: str):
    """
    下载并计算 oi 数据。
    """
    begin_date = datetime.datetime.strptime(begin, '%Y%m%d').date() if begin else datetime.date.today()
    end_date = datetime.datetime.strptime(end, '%Y%m%d').date() if end else datetime.date.today()
    dl_calc_oi_range(spot, begin_date, end_date)
    oi_csv_merge(spot)


if __name__ == '__main__':
    click_main()


