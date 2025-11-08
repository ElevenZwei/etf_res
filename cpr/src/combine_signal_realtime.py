# Load signal data from multiple sources in real-time, combine them, and output the result.

import click
import numpy as np
import pandas as pd
import datetime
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import inspect

from config import get_engine

engine = get_engine()

def load_cpr_roll_export_id(roll_args_id: int, top: int, dt: datetime.date) -> int:
    query = sa.text('''
        select id, args from cpr.roll_export
        where roll_args_id = :roll_args_id
            and top = :top
            and dt_from <= :dt
            and dt_to > :dt
        order by dt_from desc
        limit 1;
    ''')
    df = pd.read_sql(query, engine, params={
        'roll_args_id': int(roll_args_id),
        'top': int(top),
        'dt': dt,
    })
    if len(df) == 0:
        raise ValueError(f'No roll_export found for roll_args_id={roll_args_id}, top={top}, dt={dt}')
    return int(df.iloc[0]['id'])


def load_cpr_signal(dt: datetime.date, roll_export_id: int) -> pd.DataFrame:
    query = sa.text('''
        select dt, position
        from cpr.roll_export_run
        where dt between :dt and :dt + interval '1 day'
            and roll_export_id = :roll_export_id
        order by dt
    ''')
    df = pd.read_sql(query, engine, params={'dt': dt, 'roll_export_id': roll_export_id})
    return df


def load_stock_signal(dt: datetime.date) -> pd.DataFrame:
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
    df = pd.read_sql(query, engine, params={'dt': dt})
    return df


def merge_signals(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(df1, df2, on='dt', how='outer', suffixes=('_cpr', '_stock'))
    df = df.sort_values(by='dt').reset_index(drop=True)
    df['position_cpr'] = df['position_cpr'].ffill().fillna(0)
    df['position_stock'] = df['position_stock'].ffill().fillna(0)
    df['position_avg'] = (df['position_cpr'] + df['position_stock']) / 2
    df = df.set_index('dt')
    return df


# 请勿修改下面的函数签名和实现，需要保持一致以便对应数据库中的记录。
# 你只能增加新的 combine 函数，并将其添加到 combine_schema 列表中。
def amp1_row(row: pd.Series, col: str) -> int:
    """一个连续的非线性变换函数，将输入映射到 -1 到 1 之间，且在 -0.2 到 0.2 之间平滑过渡。"""
    input = row.loc[col]
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

def amp1(df: pd.DataFrame, col: str) -> pd.Series:
    """对指定列应用 amp1_row 函数。"""
    return df.apply(lambda row: amp1_row(row, col), axis=1)

def combine_amp1_avg(df: pd.DataFrame) -> pd.Series:
    """对 position_avg 列应用 amp1 函数。"""
    return amp1(df, 'position_avg')

def combine_amp1_7a3b(df: pd.DataFrame) -> pd.Series:
    """对 position_7a3b 列应用 amp1 函数。"""
    df['position_7a3b'] = df['position_cpr'] * 0.7 + df['position_stock'] * 0.3
    return amp1(df, 'position_7a3b')


default_combine_schema = [
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
]


def get_combine_schema_id(schema: list) -> list:
    """
    Upload the combine schema to the database.
    create or replace function cpr.get_or_create_combine_signal_scheme(
        scheme_name_arg text, description_arg text, code_arg text)
        returns integer language plpgsql
    """
    for item in schema:
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
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(f"Uploaded combine scheme '{item['name']}' with id {df.iloc[0]['id']}.")
        item['id'] = int(df.iloc[0]['id'])
    return schema


def upload_combine_signal(merge_df: pd.DataFrame, combine_schema: list):
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
    schema = get_combine_schema_id(combine_schema)
    for item in schema:
        sig = item['function'](merge_df)
        df = pd.DataFrame({'position': sig})
        df['scheme_id'] = item['id']
        df['product'] = '159915'
        df.to_sql('combine_signal', engine, schema='cpr',
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
                             combine_schema: list = default_combine_schema):
    reid = load_cpr_roll_export_id(
            roll_args_id=roll_args_id, top=roll_top, dt=dt)
    print('Loaded roll_export_id:', reid)
    df1 = load_cpr_signal(dt, reid)
    df2 = load_stock_signal(dt)
    df = merge_signals(df1, df2)
    upload_combine_signal(df, combine_schema)



@click.command()
@click.option('-d', '--dt', required=True, type=click.DateTime(formats=["%Y-%m-%d"]), help='Date to load signals for (YYYY-MM-DD).')
@click.option('-r', '--roll-args-id', required=False, default=1, type=int, help='Roll arguments ID for CPR signal.')
@click.option('--top', required=False, default=10, type=int, help='Top N contracts for CPR signal.')
def cli(dt, roll_args_id, top):
    load_and_combine_signals(dt.date(), roll_args_id, top, default_combine_schema)

if __name__ == '__main__':
    cli()
