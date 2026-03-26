import sqlalchemy
import polars as pl
from sqlalchemy.dialects import postgresql
from dataclasses import dataclass
from pathlib import Path

def get_file_dir():
    fpath = Path(__file__).resolve()
    return fpath.parent

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
        port=5432,
        db='opt',
)


def get_engine(config: PgConfig = PG_CPR_CONN_INFO, timeout: int = 40):
    return sqlalchemy.create_engine(sqlalchemy.URL.create(
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


class WindException(Exception):
    def __init__(self, msg, code):
        super().__init__(msg)
        self.code = code


def wind2df(wddata) -> pl.DataFrame:
    if wddata.ErrorCode != 0:
        print(f"error code: {wddata.ErrorCode}")
        raise WindException("", wddata.ErrorCode)
    res = {}
    print("columns: ", wddata.Fields, ", out_len=", len(wddata.Data))
    if len(wddata.Times) > 1:
        res['time'] = wddata.Times
    for i in range(0, len(wddata.Fields)):
        res[wddata.Fields[i]] = wddata.Data[i]
    df = pl.DataFrame(res)
    df = df.with_columns(
            pl.lit(wddata.Codes[0] if len(wddata.Codes) > 0 else None).alias('name')
    )
    return df

def wind_retry(func, max_attempts=3):
    attempts = 0
    while attempts < max_attempts:
        try:
            return func()
        except WindException as we:
            attempts += 1
            if attempts == max_attempts:
                raise we

