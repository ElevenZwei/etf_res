"""
这个文件可以更加轻松地下载 Open Interest 数据。
使用新的数据库以及经过了事先聚合的数据。
"""

import click
import datetime
import os
import sqlalchemy as sa
import pandas as pd
from typing import Optional, List

from config import DATA_DIR, get_engine

OI_DIR = f'{DATA_DIR}/fact/oi_daily_record/'
OI_MERGE_DIR = f'{DATA_DIR}/fact/oi_merge_record/'

if not os.path.exists(OI_DIR):
    os.makedirs(OI_DIR, exist_ok=True)
if not os.path.exists(OI_MERGE_DIR):
    os.makedirs(OI_MERGE_DIR, exist_ok=True)

engine: sa.engine.Engine = get_engine()


def dl_nearest_expiry(spot: str, dt: datetime.date) -> Optional[datetime.date]:
    query = sa.text("""
                    select min(expiry) as expiry
                    from md.contract_info
                    where spotcode = :spot and expiry >= :dt
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {'spot': spot, 'dt': dt})
        row = result.fetchone()
        if row is not None:
            return row[0]
        else:
            return None


def dl_oi_data(spot: str, expiry: datetime.date, oi_date: datetime.date) -> pd.DataFrame:
    query = sa.text("""
        with tradecodes as (
            select spotcode, expiry, callput, strike, tradecode
            from "md"."contract_info"
            where expiry = :expiry and spotcode = :spot
        )
        , oi_input as (
            select dt, spotcode, expiry, callput, strike, tradecode,
                oi_open, oi_close,
                lag(oi_close) over (partition by tradecode order by dt) as prev_oi_close
            from md.contract_price_minute join tradecodes using (tradecode)
            where dt >= :oi_date and dt < :oi_date + interval '1 day'
                and dt::time >= '09:30:00' and dt::time <= '15:00:00'
        )
        select dt, spotcode, expiry, callput, strike, tradecode,
            coalesce(oi_open, prev_oi_close) as oi
        from oi_input order by dt asc;
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'spot': spot,
            'expiry': expiry,
            'oi_date': oi_date
        })
    return df


def pivot_sum(df: pd.DataFrame, col: str, tag: str):
    """
    计算 call or put oi 数据的总和。
    """
    df = df.pivot(index='dt', columns='strike', values=col)
    df = df.ffill()
    # print(df)
    df.to_csv(f'{OI_DIR}/debug_cpr_pivot_sum_{tag}.csv')
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
    return df2.reset_index('dt')


def dl_spot_data(spot: str, dt_from: datetime.date, dt_to: datetime.date) -> pd.DataFrame:
    query = sa.text("""
        select dt, tradecode as code, open as openp, close as closep
        from md.contract_price_minute
        where tradecode = :spot
            and dt >= :dt_from and dt <= :dt_to
        order by dt asc;
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'spot': spot,
            'dt_from': dt_from,
            'dt_to': dt_to
        })
    return df


expiry = (dl_nearest_expiry('159915', datetime.date(2025, 10, 23)))
if expiry is not None:
    oi = (dl_oi_data('159915', expiry, datetime.date(2025, 10, 23)))
    print(oi)
    print(calc_oi(oi))
spot = dl_spot_data('159915', datetime.date(2025, 11, 3), datetime.date(2025, 11, 4))
print(spot)
