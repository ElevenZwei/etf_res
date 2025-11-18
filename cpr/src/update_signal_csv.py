# update roll_159915_<roll_args_id>.csv file from cpr.roll_merged table
# update stock_399006_avg.csv file from cpr.stock_signal table

import polars as pl
import sqlalchemy as sa
import datetime
from config import DATA_DIR, get_engine

engine = get_engine()

roll_args_ids = [1, 2]

def fetch_roll_csv(dt_from: datetime.date, roll_args_id: int):
    query = sa.text('''
                    select * from cpr.roll_merged
                    where roll_args_id = :roll_args_id
                    and dt >= :dt_from
                    order by dt
                    ''')
    with engine.connect() as conn:
        df = pl.read_database(query, conn, execute_options={'parameters': {
                  'roll_args_id': roll_args_id,
                  'dt_from': dt_from
        }})
    # 显示给出数据的格式，避免自动推断错误
    df = df.cast({'roll_args_id': pl.Int64,
                  'top': pl.Int64,
                  'dt': pl.Datetime,
                  'position': pl.Float64})
    df = df.with_columns(
            pl.col("dt").dt.convert_time_zone("Asia/Shanghai").alias("dt"),
    )
    return df


def update_roll_csv(roll_args_id: int):
    fname = f'roll_159915_{roll_args_id}.csv'
    fpath = DATA_DIR / 'signal' / fname
    dt_from = datetime.date(2025, 9, 30)
    df = pl.DataFrame()
    if fpath.exists():
        df = pl.read_csv(fpath)
        df = df.with_columns(
            pl.col("dt").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f%z")
                .dt.convert_time_zone("Asia/Shanghai").alias("dt"),
        )
        dt_max = df.select(pl.col('dt').max()).to_series()[0]
        dt_from = dt_max.date() + datetime.timedelta(days=1)
        print(f"Existing roll signal data up to {dt_max}, fetching from {dt_from} for id {roll_args_id}.")
    df_dl = fetch_roll_csv(dt_from, roll_args_id)
    if fpath.exists():
        df_all = pl.concat([df, df_dl], how='vertical')
    else:
        df_all = df_dl
    df_all.write_csv(fpath)
    print(f"fetched {len(df_dl)} rows from cpr.roll_merged with existing {len(df)} rows for id {roll_args_id} to {fname}.")


def fetch_stock_csv(dt_from: datetime.date):
    if dt_from < datetime.date(2025, 10, 15):
        print("dt_from should be no earlier than 2025-10-15 to match roll signals.")
        dt_from = datetime.date(2025, 10, 15)
    query = sa.text('''
            with input as (
                select acname, insert_time as dt, ps as position
                from cpr.stock_signal
                where product = '399006'
                    and acname in (
                      'pyelf_CybWoOacVsw_sif_1_1',
                      'pyelf_CybWoOacVswRR_sif_1_1',
                      'pyelf_CybWoOacVswRm_sif_1_1',
                      'pyelf_CybWoOacVswRmRR_sif_1_1'
                    )
                    and insert_time > :dt_from
            )
            select dt, avg(position) as position from input
            group by dt
            order by dt;
            ''')
    with engine.connect() as conn:
        df = pl.read_database(query, conn, execute_options={ 'parameters': {
                'dt_from': dt_from
        }})
    df = df.cast({'dt': pl.Datetime, 'position': pl.Float64})
    df = df.with_columns(
            pl.col("dt").dt.convert_time_zone("Asia/Shanghai").alias("dt"),
    )
    return df


def update_stock_csv():
    fname = f'stock_399006_avg_realtime.csv'
    fpath = DATA_DIR / 'signal' / fname
    dt_from = datetime.date(2025, 10, 15)
    df = pl.DataFrame()
    if fpath.exists():
        df = pl.read_csv(fpath)
        df = df.with_columns(
            pl.col("dt").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f%z")
                .dt.convert_time_zone("Asia/Shanghai").alias("dt"),
        )
        dt_max = df.select(pl.col('dt').max()).to_series()[0]
        dt_from = dt_max.date() + datetime.timedelta(days=1)
        print(f"Existing stock signal data up to {dt_max}, fetching from {dt_from}.")
    df_dl = fetch_stock_csv(dt_from)
    if not df.is_empty():
        df_all = pl.concat([df, df_dl], how='vertical')
    else:
        df_all = df_dl
    df_all.write_csv(fpath)
    print(f"fetched {len(df_dl)} rows from cpr.stock_signal with existing {len(df)} rows to {fname}.")


def concat_stock_csv():
    fname1 = f'stock_399006_avg_archive.csv'
    fname2 = f'stock_399006_avg_realtime.csv'
    df1 = pl.read_csv(DATA_DIR / 'signal' / fname1)
    df2 = pl.read_csv(DATA_DIR / 'signal' / fname2)
    df = pl.concat([df1, df2], how='vertical').unique(subset=['dt']).sort('dt')
    df.write_csv(DATA_DIR / 'signal' / 'stock_399006_avg.csv')
    print(f"Concatenated stock signal to stock_399006_avg.csv with {len(df)} rows.")



if __name__ == '__main__':
    for roll_args_id in roll_args_ids:
        update_roll_csv(roll_args_id)
    update_stock_csv()
    concat_stock_csv()



