"""
这个文件的目标是读取 parquet 文件并写入 SQL 数据库。

create table if not exists cpr.stock_signal_import (
    dt timestamptz not null,
    name text not null,  -- strategy name
    code text not null,  -- stock code
    position float8 not null,  -- position [-1, 1]
    check (position >= -1 and position <= 1)
);
"""

import polars as pl
import datetime

from config import DATA_DIR, get_engine, upsert_on_conflict_skip


def load_parquet(file_path: str) -> pl.DataFrame:
    """
    读取 parquet 文件，进行预处理，并返回 polars DataFrame。
    """
    df = pl.read_parquet(file_path)
    df = df.rename({ '__index_level_0__': 'dt', '399006': 'position' })
    basename = file_path.split('/')[-1].replace('.parquet', '')
    df = df.with_columns([
        pl.col('dt').cast(pl.Datetime).dt.replace_time_zone('Asia/Shanghai'),
        pl.lit(basename).alias('name'),
        pl.lit('399006').alias('code'),
    ])
    # forward fill in the same day
    df = df.sort(['dt']).with_columns([
        pl.col('position')
            .fill_null(strategy='forward')
            .over(pl.col('dt').dt.date())
            .fill_null(0)
            .alias('position')
    ])
    # fill ps to 0 if insert_time after 14:51
    df = df.with_columns([
        pl.when(pl.col('dt').dt.time() >= datetime.time(14, 51))
          .then(0.0)
          .otherwise(pl.col('position'))
          .alias('position')
    ])
    print(df.head())
    print(df.tail())
    return df


def write_to_db(df: pl.DataFrame):
    """
    将 polars DataFrame 写入 SQL 数据库。
    """
    engine = get_engine()
    df_pd = df.to_pandas()
    df_pd.to_sql('stock_signal_import', engine, schema='cpr',
                 if_exists='append', index=False,
                 method=upsert_on_conflict_skip,
                 chunksize=1000)
    print(f"Wrote {len(df_pd)} rows to cpr.stock_signal_import.")


def load_and_write(file_path: str):
    df = load_parquet(file_path)
    write_to_db(df)
    return df

def select_from_db(name: str, dt_from: datetime.date, dt_to: datetime.date) -> pl.DataFrame:
    engine = get_engine()
    query = """
        select * from cpr.stock_signal_import
        where name = :name
            and code = '399006'
            and dt >= :dt_from and dt <= :dt_to
        order by dt;
    """
    with engine.connect() as conn:
        df_pd = pl.read_database(query, conn, execute_options={
            'parameters': {
                'name': name,
                'dt_from': dt_from,
                'dt_to': dt_to
            }
        })
        print(df_pd)
    return df_pd

def select_avg_from_db(dt_from: datetime.date, dt_to: datetime.date) -> pl.DataFrame:
    engine = get_engine()
    query = """
        select dt, avg(position) as position
        from cpr.stock_signal_import
        where dt >= :dt_from and dt <= :dt_to
            and code = '399006'
            and name in (
              'pyelf_CybWoOacVsw_sif_1_1',
              'pyelf_CybWoOacVswRR_sif_1_1',
              'pyelf_CybWoOacVswRm_sif_1_1',
              'pyelf_CybWoOacVswRmRR_sif_1_1'
            )
        group by dt
        order by dt;
    """
    with engine.connect() as conn:
        df_pd = pl.read_database(query, conn, execute_options={
            'parameters': {
                'dt_from': dt_from,
                'dt_to': dt_to
            }
        })
        df_pd = df_pd.cast({'dt': pl.Datetime, 'position': pl.Float64})
        df_pd = df_pd.with_columns(
                pl.col("dt").dt.convert_time_zone("Asia/Shanghai").alias("dt"),
        )
        print(df_pd)
    return df_pd

if __name__ == "__main__":
    # load_and_write(f'{DATA_DIR}/signal/cyb_position/pyelf_CybWoOacVswRmRR_sif_1_1.parquet')
    # load_and_write(f'{DATA_DIR}/signal/cyb_position/pyelf_CybWoOacVswRm_sif_1_1.parquet')
    # load_and_write(f'{DATA_DIR}/signal/cyb_position/pyelf_CybWoOacVswRR_sif_1_1.parquet')
    # load_and_write(f'{DATA_DIR}/signal/cyb_position/pyelf_CybWoOacVsw_sif_1_1.parquet')
    # select_from_db('pyelf_CybWoOacVswRmRR_sif_1_1', datetime.date(2025, 10, 10), datetime.date(2025, 10, 15))
    df = select_avg_from_db(datetime.date(2025, 1, 1), datetime.date(2025, 10, 15))
    df.write_csv(f'{DATA_DIR}/signal/stock_399006_avg_archive.csv')
