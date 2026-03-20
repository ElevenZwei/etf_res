"""
读取 'data/all_future_daily_price.csv' 文件中的期货日线数据，并将其导入到 PostgreSQL 数据库中。
CSV 文件中的数据格式如下：
tradecode,dt,open,high,low,close
导入过程中会对 tradecode 进行处理，去掉交易所后缀，并根据交易所类型调整大小写。
例如，'IF2106.CZC' 会被处理为 'IF2106'，'RB2105.SHF' 会被处理为 'rb2105'。
数据库中使用了一个存储过程 md.set_contract_daily_price 来插入或更新日线数据。
"""

import polars as pl
import sqlalchemy
from dataclasses import dataclass
from tqdm import tqdm

@dataclass(frozen=True)
class PgConfig:
    user: str
    pw: str
    host: str
    port: int
    db: str

# 数据库连接配置
# 有两个地址，一个内网地址，一个公网地址
# 内网地址是 172.16.30.6:5432
# 公网地址是 124.222.94.46:19018
# 内网地址连接更快更稳定，先尝试内网地址，失败后再尝试公网地址
PG_CONN_INFO = PgConfig(
        user='option',
        pw='option',
        host='172.16.30.6',
        port=5432,
        # host='124.222.94.46',
        # port=19018,
        db='opt',
)

def get_engine(config: PgConfig = PG_CONN_INFO, timeout: int = 10):
    return sqlalchemy.create_engine(sqlalchemy.URL.create(
        'postgresql',
        username=config.user,
        password=config.pw,
        host=config.host,
        port=config.port,
        database=config.db,
        query={
            'sslmode': 'disable',
            'connect_timeout': str(timeout),
        },
    ))

engine = get_engine()

def insert_daily_price(
    tradecode: str, date: str,
    open: float, high: float, low: float, close: float):
    if open is None and high is None and low is None and close is None:
        return None
    """
    create or replace function md.set_contract_daily_price(
        tradecode_arg text, date_arg date,
        open_arg float8, high_arg float8, low_arg float8, close_arg float8)
        returns integer language plpgsql as $$
    """
    query = sqlalchemy.text("""
        select md.set_contract_daily_price(
            :tradecode_arg, :date_arg,
            :open_arg, :high_arg, :low_arg, :close_arg)
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {
            'tradecode_arg': tradecode,
            'date_arg': date,
            'open_arg': open,
            'high_arg': high,
            'low_arg': low,
            'close_arg': close,
        })
        conn.commit()
        return result.scalar()


def import_daily_price(df: pl.DataFrame):
    for row in tqdm(df.iter_rows(named=True), total=df.height):
        wind_code = row['tradecode']
        exchange = wind_code.split('.')[-1]
        tradecode = wind_code.split('.')[0]
        if exchange != 'CZC':
            tradecode = tradecode[:-2].lower() + tradecode[-2:]
        else:
            tradecode = tradecode.upper()
        insert_daily_price(
            tradecode=tradecode,
            date=row['dt'],
            open=row['open'],
            high=row['high'],
            low=row['low'],
            close=row['close'],
        )


df = pl.read_csv('data/all_future_daily_price.csv')

import_daily_price(df)