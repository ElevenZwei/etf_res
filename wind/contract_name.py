"""
这个文件提供了一个函数，根据交易代码获取对应的Wind代码。
这个函数从数据库里面寻找交易所信息，并将其附加到交易代码上，形成完整的Wind代码。
"""

import polars as pl
import sqlalchemy as sa
# from sqlalchemy.dialects import postgresql

from header import get_engine

engine = get_engine()

def dl_contract_info() -> pl.DataFrame:
    '''
    '''
    query = sa.text('''
        select tradecode, name, exchange, expiry from md.contract_info
    ''')
    with engine.connect() as conn:
        df = pl.read_database(query, conn, execute_options={})
    df = df.cast({
        'tradecode': pl.Utf8,
        'name': pl.Utf8,
        'exchange': pl.Utf8,
        'expiry': pl.Date
    })
    return df;

contract_info = dl_contract_info()

def get_wind_code(tradecode: str):
    exchange = (contract_info
            .filter(pl.col('tradecode') == pl.lit(tradecode))
            .select(pl.col('exchange'))
            .to_series()[0])
    return tradecode + '.' + exchange[:3]


# print(get_wind_code('ad2601'))

