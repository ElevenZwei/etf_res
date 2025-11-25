# Load signal data from multiple sources in real-time, combine them, and output the result.
# upsert the combined signal into the database.

import click
import numpy as np
import pandas as pd
import polars as pl
import datetime
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import inspect
from typing import List

from config import get_engine

engine = get_engine()

def load_cpr_roll_export_id(roll_args_id: int, top: int, dt: datetime.date) -> int:
    query = sa.text('''
        select id from cpr.roll_export
        where roll_args_id = :roll_args_id
            and top = :top
            and dt_from <= :dt
            and dt_to > :dt
        order by dt_from desc
        limit 1;
    ''')
    with engine.connect() as conn:
        df = pl.read_database(query, conn, execute_options={
            'parameters': {
                'roll_args_id': int(roll_args_id),
                'top': int(top),
                'dt': dt,
            }
        })
    if len(df) == 0:
        raise ValueError(f'No roll_export found for roll_args_id={roll_args_id}, top={top}, dt={dt}')
    return int(df.select(pl.col('id')).to_series()[0])


def load_cpr_signal(dt: datetime.date, roll_export_id: int) -> pl.DataFrame:
    query = sa.text('''
        select dt, position
        from cpr.roll_export_run
        where dt between :dt and :dt + interval '1 day'
            and roll_export_id = :roll_export_id
        order by dt
    ''')
    with engine.connect() as conn:
        df = pl.read_database(query, conn, execute_options={
            'parameters': {
                'dt': dt,
                'roll_export_id': roll_export_id
            }
        })
    return df


def load_stock_signal(dt: datetime.date) -> pl.DataFrame:
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
                and insert_time between :dt and :dt + interval '1 day'
        )
        select dt, avg(position) as position from input
        group by dt
        order by dt;
    ''')
    with engine.connect() as conn:
        df = pl.read_database(query, conn, execute_options={
            'parameters': { 'dt': dt }})
    return df


def merge_signals(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    df1 = df1.rename({ 'position': 'position_cpr' })
    df2 = df2.rename({ 'position': 'position_stock' })
    # 有一些时候可能有的信号有遗漏 dt = coalesce(df1.dt, df2.dt)
    df = df1.join(df2, on='dt', how='full', suffix='_stock')
    df = df.with_columns(
            pl.coalesce(pl.col('dt'), pl.col('dt_stock')).alias('dt')
    ).drop('dt_stock')
    df = df.sort('dt')
    df = df.with_columns([
        pl.col('position_cpr').forward_fill().fill_null(0),
        pl.col('position_stock').forward_fill().fill_null(0),
    ])
    df = df.with_columns([
        ((pl.col('position_cpr') + pl.col('position_stock')) / 2).alias('position_avg'),
        (pl.col('position_cpr') * 0.7 + pl.col('position_stock') * 0.3).alias('position_7a3b'),
        (pl.col('position_cpr') - pl.col('position_stock')).alias('position_diff'),
        (pl.col('position_cpr') - pl.col('position_stock')).abs().alias('position_diff_abs'),
    ])
    return df


# 请勿修改下面的函数签名和实现，需要保持一致以便对应数据库中的记录。
# 你只能增加新的 combine 函数，并将其添加到 combine_schemes 列表中。
def amp1_row(row: dict, col: str) -> float:
    """一个连续的非线性变换函数，将输入映射到 -1 到 1 之间，且在 -0.2 到 0.2 之间平滑过渡。"""
    input = row[col]
    if input < -0.4:
        return np.maximum(input * 2, -1)
    if input < -0.2:
        multiplier = (-0.2 - input) / 0.2 * 1 + 1
        return np.maximum(input * multiplier, -1)
    if input > 0.4:
        return np.minimum(input * 2, 1)
    if input > 0.2:
        multiplier = (input - 0.2) / 0.2 * 1 + 1
        return np.minimum(input * multiplier, 1)
    return input * abs(input) / 0.2

def amp1(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """对指定列应用 amp1_row 函数。"""
    df = df.with_columns(
            pl.struct([col]).map_elements(
                lambda row: amp1_row(row, col),
                return_dtype=pl.Float64,
            ).alias('position')
    )
    return df.select([pl.col('dt'), pl.col('position')])

def combine_amp1_avg(df: pl.DataFrame) -> pl.DataFrame:
    """对 position_avg 列应用 amp1 函数。"""
    return amp1(df, 'position_avg')

def combine_amp1_7a3b(df: pl.DataFrame) -> pl.DataFrame:
    """对 position_7a3b 列应用 amp1 函数。"""
    return amp1(df, 'position_7a3b')


def amp2_row(row: dict, col: str, diff_col: str) -> float:
    """一个连续的非线性变换函数，将输入映射到 -1 到 1 之间，且在 -0.2 到 0.2 之间平滑过渡。"""
    input = row[col]
    multiplier = 1.0
    diff = row[diff_col]
    if diff > 1.4:
        return 0
    if diff > 1.2:
        multiplier *= 1 - (diff - 1.2) / 0.2

    if input < -0.4:
        multiplier *= 2.0
        return np.maximum(input * multiplier, -1)
    if input < -0.2:
        multiplier = (-0.2 - input) / 0.2 * 1 + 1
        return np.maximum(input * multiplier, -1)
    if input > 0.4:
        multiplier *= 2.0
        return np.minimum(input * multiplier, 1)
    if input > 0.2:
        multiplier = (input - 0.2) / 0.2 * 1 + 1
        return np.minimum(input * multiplier, 1)

    return multiplier * input * abs(input) / 0.2


def amp2(df: pl.DataFrame, col: str, diff_col: str) -> pl.DataFrame:
    df = df.with_columns(
            pl.struct([col, diff_col]).map_elements(
                lambda row: amp2_row(row, col, diff_col),
                return_dtype=pl.Float64,
            ).alias('position')
    )
    return df.select([pl.col('dt'), pl.col('position')])

def combine_amp2_avg(df: pl.DataFrame) -> pl.DataFrame:
    return amp2(df, 'position_avg', 'position_diff_abs')

def combine_amp2_7a3b(df: pl.DataFrame) -> pl.DataFrame:
    return amp2(df, 'position_7a3b', 'position_diff_abs')


default_combine_schemes = [
    {
        'name': 'amp1_avg',
        'description': 'Combine amp1 using average of CPR and stock signals.',
        'function': combine_amp1_avg,
        'code': inspect.getsource(combine_amp1_avg)
                + '\n' + inspect.getsource(amp1)
                + '\n' + inspect.getsource(amp1_row),
    },
    {
        'name': 'amp1_7a3b',
        'description': 'Combine amp1 using 70% CPR and 30% stock signals.',
        'function': combine_amp1_7a3b,
        'code': inspect.getsource(combine_amp1_7a3b)
                + '\n' + inspect.getsource(amp1)
                + '\n' + inspect.getsource(amp1_row),
    },
    {
        'name': 'amp2_avg',
        'description': 'Combine amp2 using average of CPR and stock signals.',
        'function': combine_amp2_avg,
        'code': inspect.getsource(combine_amp2_avg)
                + '\n' + inspect.getsource(amp2)
                + '\n' + inspect.getsource(amp2_row),
    },
    {
        'name': 'amp2_7a3b',
        'description': 'Combine amp2 using 70% CPR and 30% stock signals.',
        'function': combine_amp2_7a3b,
        'code': inspect.getsource(combine_amp2_7a3b)
                + '\n' + inspect.getsource(amp2)
                + '\n' + inspect.getsource(amp2_row),
    },
]


def combine_schemes_with_fetched_id(schemes: list) -> list:
    """
    Upload the combine schemes to the database.
    create or replace function cpr.get_or_create_combine_signal_scheme(
        scheme_name_arg text, description_arg text, code_arg text)
        returns integer language plpgsql
    """
    for item in schemes:
        query = sa.text('''
            select cpr.get_or_create_combine_signal_scheme(
                :scheme_name_arg,
                :description_arg,
                :code_arg
            ) as id;
        ''')
        with engine.connect() as conn:
            result = conn.execute(query, {
                'scheme_name_arg': item['name'],
                'description_arg': item['description'],
                'code_arg': item['code'],
            })
            conn.commit()
            columns: List[str] = list(result.keys())
            df = pl.DataFrame(result.fetchall(), schema=columns)
        item_id = df.select(pl.col('id')).to_series()[0]
        print(f"Uploaded combine scheme '{item['name']}' with id {item_id}")
        item['id'] = int(item_id)
    return schemes


def upload_combine_signal(merge_df: pl.DataFrame, combine_schemes: list):
    """
    Upload the combined signals to the database.
    create table if not exists cpr.combine_signal (
        id serial primary key,
        scheme_id integer not null references cpr.combine_signal_scheme(id) on delete cascade,
        dt timestamptz not null,
        product text not null,
        position float8 not null default 0,
        inserted_at timestamptz not null default now(),
        check(position >= -1 and position <= 1)
    );
    """
    schemes_with_id = combine_schemes_with_fetched_id(combine_schemes)
    for item in schemes_with_id:
        df: pl.DataFrame = item['function'](merge_df)
        df = df.with_columns([
            pl.lit(item['id']).alias('scheme_id'),
            pl.lit('159915').alias('product'),
        ])
        print(df)
        pd_df: pd.DataFrame = df.to_pandas().set_index('dt')
        # print(pd_df.head())
        pd_df.to_sql('combine_signal', engine, schema='cpr',
                if_exists='append', index_label='dt',
                method=upsert_combine_signal)
        print('Uploaded combine signal for scheme:', item['name'], 'rows:', len(df))


def upsert_combine_signal(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = postgresql.insert(table.table).values(data)
        update_dict = {
                'position': stmt.excluded.position,
        }
        condition = (stmt.excluded.position != table.table.c.position)
        stmt = stmt.on_conflict_do_update(
                index_elements=['scheme_id', 'dt', 'product'],
                set_=update_dict,
                where=condition)
        conn.execute(stmt)


def load_and_combine_signals(dt: datetime.date,
                             roll_args_id: int, roll_top: int,
                             combine_schemes: list = default_combine_schemes):
    reid = load_cpr_roll_export_id(
            roll_args_id=roll_args_id, top=roll_top, dt=dt)
    print('Loaded roll_export_id:', reid)
    df1 = load_cpr_signal(dt, reid)
    df2 = load_stock_signal(dt)
    df = merge_signals(df1, df2)
    upload_combine_signal(df, combine_schemes)



@click.command()
@click.option('-d', '--dt', required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help='Date to load signals for (YYYY-MM-DD).')
@click.option('-r', '--roll-args-id', required=False, default=1, type=int, help='Roll arguments ID for CPR signal.')
@click.option('--top', required=False, default=10, type=int, help='Top N contracts for CPR signal.')
def cli(dt, roll_args_id, top):
    load_and_combine_signals(dt.date(), roll_args_id, top, default_combine_schemes)

if __name__ == '__main__':
    cli()
    # combine_schemes_with_fetched_id(default_combine_schemes)
