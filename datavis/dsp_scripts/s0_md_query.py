# 从 market_data_tick 数据表里面查询 OI 数据并且储存在 csv 文件里。

from typing import Optional
import click
import datetime
from dateutil.relativedelta import relativedelta
import sqlalchemy
import pandas as pd
from dsp_config import DATA_DIR

def get_engine():
    return sqlalchemy.create_engine('postgresql+psycopg2://option:option@localhost:15432/opt')

def dl_expiry_date(spot: str, year: int, month: int) -> Optional[datetime.date]:
    d_from = datetime.datetime(year, month, 1)
    d_to = datetime.datetime(year, month, 28)
    query = f"""
        select min(expirydate) as expirydate
        from contract_info ci
        where spotcode = '{spot}'
        and expirydate >= '{d_from.strftime('%Y-%m-%d')}'
        and expirydate <= '{d_to.strftime('%Y-%m-%d')}'
    """
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn)
    if df.shape[0] == 0:
        return None
    return df['expirydate'].iloc[0]

def dl_oi_data(spot: str, expiry_date: datetime.date, md_date: datetime.date):
    expiry_date_str = expiry_date.strftime('%Y-%m-%d')
    md_str = md_date.strftime('%Y-%m-%d')
    bg_time = '09:30:00'
    ed_time = '15:30:00'
    query = f"""
        set enable_nestloop=false;
        with OI as (
            select *, oi - oi_open as oi_diff, log(oi) - log(oi_open) as oi_dlog
            from (
                select
                    dt, spotcode, expirydate, callput, strike, tradecode,
                    open_interest as oi,
                    first_value(open_interest) over (partition by code order by dt) as oi_open
                from market_data_tick mdt join contract_info ci using(code)
                where dt >= '{f"{md_str} {bg_time}"}' and dt <= '{f"{md_str} {ed_time}"}'
                and spotcode = '{spot}' and expirydate = '{expiry_date_str}'
            ) as T
        ),
        OD as (
            select
                oi1.dt, spotcode, expirydate, strike,
                oi1.tradecode as oi1c, oi2.tradecode as oi2c,
                oi1.oi_open as oi_open_c, oi2.oi_open as oi_open_p,
                oi1.oi_diff as oi_diff_c, oi2.oi_diff as oi_diff_p,
                oi1.oi_dlog as oi_dlog_c, oi2.oi_dlog as oi_dlog_p
            from OI oi1 join OI oi2 using (dt, spotcode, expirydate, strike)
            where oi1.callput = 1 and oi2.callput = -1
        )
        select od.*, mdt.last_price as spot_price
        from OD join market_data_tick mdt using (dt)
        where dt >= '{f"{md_str} {bg_time}"}' and dt <= '{f"{md_str} {ed_time}"}'
        and mdt.code = od.spotcode
        order by dt asc;
    """

    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn)
    
    if df.shape[0] == 0:
        raise RuntimeError("db is empty.")

    df['dt'] = df['dt'].dt.tz_convert('Asia/Shanghai')
    df['dt'] = df['dt'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    # print(df)
    # print(df['dt'])
    # df.to_csv('test.csv', index=False)
    md_date_str = md_date.strftime('%Y%m%d')
    df.to_csv(f'{DATA_DIR}/dsp_input/strike_oi_diff_{spot}_{md_date_str}.csv',
            index=False)

def get_nearest_expirydate(spot: str, dt: datetime.datetime):
    exp: Optional[datetime.date] = dl_expiry_date(spot, dt.year, dt.month)
    if exp is None:
        return exp
    # 对于当月交割日已经过去的情况，切换到下一个月
    if exp < dt.date():
        dt += relativedelta(months=1)
        exp = dl_expiry_date(spot, dt.year, dt.month)
    return exp

def auto_dl(spot: str, md_date: str, year: Optional[int] = None, month: Optional[int] = None):
    dt = datetime.datetime.strptime(md_date, '%Y%m%d')
    if year is None or month is None:
        exp = get_nearest_expirydate(spot, dt)
    else:
        exp = dl_expiry_date(spot, year, month)
    if exp is None:
        exit(1)
    dl_oi_data(spot, exp, dt)

@click.command()
@click.option('-s', '--spot', type=str)
@click.option('-y', '--year', type=int)
@click.option('-m', '--month', type=int)
@click.option('-d', '--date', type=str, help="format is %Y%m%d")
def click_main(spot: str, year: int, month: int, date: str):
    auto_dl(spot=spot, md_date=date, year=year, month=month)

if __name__ == '__main__':
    click_main()
    