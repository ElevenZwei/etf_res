# fix contract_price_tick.csv from old table format to new table format

import polars as pl

df = pl.read_csv('../data/dump/contract_price_tick.csv')
empty = pl.lit(None)
df = df.with_columns([
    empty.alias('ask4_price'),
    empty.alias('bid4_price'),
    empty.alias('ask4_size'),
    empty.alias('bid4_size'),
    empty.alias('ask5_price'),
    empty.alias('bid5_price'),
    empty.alias('ask5_size'),
    empty.alias('bid5_size'),
]).select([
    'tradecode',
    'dt',
    'last_price',
    'vol',
    'oi',
    'ask_price',
    'bid_price',
    'ask_size',
    'bid_size',
    'ask2_price',
    'bid2_price',
    'ask2_size',
    'bid2_size',
    'ask3_price',
    'bid3_price',
    'ask3_size',
    'bid3_size',
    'ask4_price',
    'bid4_price',
    'ask4_size',
    'bid4_size',
    'ask5_price',
    'bid5_price',
    'ask5_size',
    'bid5_size',
    'inserted_at',
])

df.write_csv('../data/dump/contract_price_tick_fixed.csv')


