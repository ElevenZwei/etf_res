# 从 market_data_tick 数据表里面查询 OI 数据并且储存在 csv 文件里。

from typing import Optional
from collections import deque
import click
import datetime
from dateutil.relativedelta import relativedelta
import io
import os
import sqlalchemy
import pandas as pd

from dsp_config import DATA_DIR, PG_DB_CONF, gen_suffix

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

def get_engine():
    return sqlalchemy.create_engine(sqlalchemy.URL.create(
        'postgresql+psycopg2',
        username=PG_DB_CONF.user,
        password=PG_DB_CONF.pw,
        host=PG_DB_CONF.host,
        port=PG_DB_CONF.port,
        database=PG_DB_CONF.db,
    ))

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
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn)
    if df.shape[0] == 0:
        return None
    return df['expirydate'].iloc[0]

def dl_oi_data(spot: str, expiry_date: datetime.date,
        bg_date: datetime.date, ed_date: datetime.date,
        bg_time: datetime.time = datetime.time(9, 30, 0),
        ed_time: datetime.time = datetime.time(15, 0, 0),
        minute_bar: bool = False) -> pd.DataFrame:
    expiry_date_str = expiry_date.strftime('%Y-%m-%d')
    bg_date_str = bg_date.strftime('%Y-%m-%d')
    ed_date_str = ed_date.strftime('%Y-%m-%d')
    bg_time_str = bg_time.strftime('%H:%M:%S')
    ed_time_str = ed_time.strftime('%H:%M:%S')
    if minute_bar:
        if spot == '159915':
            suffix = '.SZ'
        else:
            suffix = '.SH'
        query = f"""
            set enable_nestloop=false;
            with syms as (
                select code, tradecode, spotcode, expirydate, strike, callput
                from contract_info ci 
                where ci.spotcode = '{spot}{suffix}'
                and ci.expirydate = '{expiry_date_str}'
                and code like '%%{suffix}'),
            opt_mdt as (
                select
                dt, code, tradecode,
                spotcode, expirydate, strike, callput,
                openinterest as oi
                -- , first_value(openinterest) over (partition by code order by dt) as oi_open
                from market_data dt join syms using(code)
                where dt > '{bg_date_str} {bg_time_str}' and dt < '{ed_date_str} {ed_time_str}'),
            opt_oi as (
                select dt, spotcode, expirydate, strike
                , om1.tradecode as oi1c, om2.tradecode as oi2c
                , om1.oi as oi_c, om2.oi as oi_p
                -- , om1.oi_open as oi_open_c, om2.oi_open as oi_open_p
                -- , om1.oi - om1.oi_open as oi_diff_c, om2.oi - om2.oi_open as oi_diff_p
                from opt_mdt om1 join opt_mdt om2 using(dt, spotcode, expirydate, strike)
                where om1.callput = 1 and om2.callput = -1)
            select opt_oi.*, md.closep as spot_price
            from opt_oi join market_data md using(dt)
            where dt > '{bg_date_str} {bg_time_str}' and dt < '{ed_date_str} {ed_time_str}'
            and md.code = opt_oi.spotcode
            order by dt asc;
        """
    else:
        # TODO 我现在怀疑这个查询对于不经常更新的期权，这个 OD match 的过程会遗漏很多项目。
        # 不过这种遗漏只会在 wind 下载的数据里面出现，因为录制机器会重复相同的数据。
        # 这有两个改进的方向，一个是在查询的时候把精度降低到 30 秒，假设 30 秒里面各种项目至少有一次更新。
        # 另一个是把这个 pivot + ffill + join 的逻辑放到 python 里面来做。
        query = f"""
            set enable_nestloop=false;
            with OI as (
                select *
                -- , oi - oi_open as oi_diff
                -- , log(oi) - log(oi_open) as oi_dlog
                from (
                    select
                        dt, spotcode, expirydate, callput, strike, tradecode,
                        open_interest as oi
                        -- , first_value(open_interest) over (partition by code order by dt) as oi_open
                    from market_data_tick mdt join contract_info ci using(code)
                    where dt >= '{f"{bg_date_str} {bg_time_str}"}' and dt <= '{f"{ed_date_str} {ed_time_str}"}'
                    and dt::time >= '09:30:00' and dt::time <= '15:00:00'
                    and spotcode = '{spot}' and expirydate = '{expiry_date_str}'
                ) as T
            ),
            OD as (
                select
                    oi1.dt, spotcode, expirydate, strike
                    , oi1.tradecode as oi1c, oi2.tradecode as oi2c
                    , oi1.oi as oi_c, oi2.oi as oi_p
                    -- , oi1.oi_open as oi_open_c, oi2.oi_open as oi_open_p
                    -- , oi1.oi_diff as oi_diff_c, oi2.oi_diff as oi_diff_p
                    -- , oi1.oi_dlog as oi_dlog_c, oi2.oi_dlog as oi_dlog_p
                from OI oi1 join OI oi2 using (dt, spotcode, expirydate, strike)
                where oi1.callput = 1 and oi2.callput = -1
            )
            select od.*, mdt.last_price as spot_price
            from OD join market_data_tick mdt using (dt)
            where dt >= '{f"{bg_date_str} {bg_time_str}"}' and dt <= '{f"{ed_date_str} {ed_time_str}"}'
            and mdt.code = od.spotcode
            order by dt asc;
        """
    
    print(query)
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn)
    
    if df.shape[0] == 0:
        raise RuntimeError("db is empty.")

    df['dt'] = df['dt'].dt.tz_convert('Asia/Shanghai')
    df['dt'] = df['dt'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    return df
    
def df_calc_open_diff(df: pd.DataFrame) -> pd.DataFrame:
    call_df = df.pivot(index='dt', columns='strike', values='oi_c')
    call_df = call_df.ffill().bfill()
    call_df = call_df - call_df.iloc[0]
    call_melt = call_df.melt(ignore_index=False, var_name='strike', value_name='oi_diff_c')
    put_df = df.pivot(index='dt', columns='strike', values='oi_p')
    put_df = put_df.ffill().bfill()
    put_df = put_df - put_df.iloc[0]
    put_melt = put_df.melt(ignore_index=False, var_name='strike', value_name='oi_diff_p')
    df2 = pd.merge(call_melt, put_melt, left_on=['dt', 'strike'], right_on=['dt', 'strike'], how='inner')
    # print(df2)
    # exit(1)
    df = pd.merge(df, df2, left_on=['dt', 'strike'], right_on=['dt', 'strike'], how='inner')
    return df

def dl_save_range_oi(spot: str, expiry_date: datetime.date,
        bg_date: datetime.date, ed_date: datetime.date,
        minute_bar: bool):
    bg_date_str = bg_date.strftime('%Y%m%d')
    ed_date_str = ed_date.strftime('%Y%m%d')
    expiry_date_str = expiry_date.strftime('%Y%m%d')
    date_suffix = bg_date_str if bg_date == ed_date else f'{bg_date_str}_{ed_date_str}'
    suffix = gen_suffix(expiry_date_str, date_suffix)
    fname = f'strike_oi_raw_{spot}_{suffix}.csv'
    fpath = f'{DATA_DIR}/dsp_input/{fname}'

    if os.path.exists(fpath):
        last_row = read_last_row(fpath)
        last_dt = pd.to_datetime(last_row['dt'])
        bg_date = last_dt.date()
        bg_time = (last_dt + datetime.timedelta(seconds=1)).time()
    else:
        bg_time = datetime.time(9, 30, 0)
    df2 = dl_oi_data(spot, expiry_date,
            bg_date, ed_date,
            bg_time=bg_time, ed_time=datetime.time(15, 0, 0),
            minute_bar=minute_bar)

    if os.path.exists(fpath):
        df1 = pd.read_csv(fpath)
        df = pd.concat([df1, df2], ignore_index=True)
    else:
        df = df2
    df.to_csv(fpath, index=False)

    df_diff = df_calc_open_diff(df)
    fname_diff = f'strike_oi_diff_{spot}_{suffix}.csv'
    fpath_diff = f'{DATA_DIR}/dsp_input/{fname_diff}'
    df_diff.to_csv(fpath_diff, index=False)
    return suffix

def get_nearest_expirydate(spot: str, dt: datetime.datetime):
    exp: Optional[datetime.date] = dl_expiry_date(spot, dt.year, dt.month)
    if exp is None:
        return exp
    # 对于当月交割日已经过去的情况，切换到下一个月
    if exp < dt.date():
        dt += relativedelta(months=1)
        exp = dl_expiry_date(spot, dt.year, dt.month)
    return exp

def auto_dl(spot: str, bg_str: str, ed_str: str,
        year: Optional[int] = None, month: Optional[int] = None,
        minute_bar=False):
    bg_dt = datetime.datetime.strptime(bg_str, '%Y%m%d')
    ed_dt = datetime.datetime.strptime(ed_str, '%Y%m%d')
    if year is None or month is None:
        exp = get_nearest_expirydate(spot, ed_dt)
    else:
        exp = dl_expiry_date(spot, year, month)
    if exp is None:
        print("cannot find expiry date.")
        exit(1)
    return dl_save_range_oi(spot, exp,
            bg_date=bg_dt, ed_date=ed_dt, minute_bar=minute_bar)

@click.command()
@click.option('-s', '--spot', type=str)
@click.option('-y', '--year', type=int)
@click.option('-m', '--month', type=int)
@click.option('-d', '--date', type=str, help="format is %Y%m%d")
@click.option('--bar', is_flag=True, help="download data from minute bar table.")
def click_main(spot: str, year: int, month: int, date: str, bar: bool):
    auto_dl(spot=spot, bg_str=date, ed_str=date, year=year, month=month, minute_bar=bar)

if __name__ == '__main__':
    click_main()
    