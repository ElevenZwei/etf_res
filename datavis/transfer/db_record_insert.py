import datetime
import glob
import pandas as pd
import sqlalchemy as sa
import tqdm
from sqlalchemy.dialects import postgresql
from dataclasses import dataclass

@dataclass(frozen=True)
class PgConfig:
    user: str
    pw: str
    host: str
    port: int
    db: str

PG_CPR_CONN_INFO = PgConfig(
        user='option',
        pw='option',
        host='localhost',
        port=25432,
        db='opt',
)

def get_engine(config: PgConfig = PG_CPR_CONN_INFO, timeout: int = 40):
    return sa.create_engine(sa.URL.create(
        'postgresql',
        username=config.user,
        password=config.pw,
        host=config.host,
        port=config.port,
        database=config.db,
        query={
            'sslmode': 'require' if config.host != 'localhost' else 'disable',
            'connect_timeout': str(timeout),
        },
    ))

def upsert_on_conflict_skip(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = postgresql.insert(table.table).values(data)
    stmt = stmt.on_conflict_do_nothing()
    conn.execute(stmt)


def record_files(spot: str, dt: datetime.date):
    dtstr = dt.strftime('%Y%m%d')
    file_pattern = f'../db/tick/record_{spot}_*_{dtstr}.csv'
    files = glob.glob(file_pattern)
    return files


def insert_record_file(engine, df: pd.DataFrame):
    with engine.begin() as conn:
        df.to_sql('contract_price_tick', conn, schema='md',
            if_exists='append', index=False,
            method=upsert_on_conflict_skip,
            chunksize=1000,
        )
        print(f"inserted {df.shape[0]} rows to contract_price_tick")


def insert_record_files(engine, spot: str, dt: datetime.date):
    files = record_files(spot, dt)
    for file_path in tqdm.tqdm(files, desc='insert record files'):
        insert_record_file(engine, pd.read_csv(file_path))


if __name__ == '__main__':
    spot = '159915.SZ'
    # dt = datetime.date(2025, 11, 25)
    dt = datetime.date(2025, 11, 28)
    engine = get_engine()
    insert_record_files(engine, spot, dt)