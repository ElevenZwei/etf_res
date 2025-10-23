import polars as pl


# fix contract_price_tick.csv from old table format to new table format
def fix_contract_price_tick():
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
        'last_price', 'vol', 'oi',
        'ask_price', 'bid_price',
        'ask_size', 'bid_size',
        'ask2_price', 'bid2_price',
        'ask2_size', 'bid2_size',
        'ask3_price', 'bid3_price',
        'ask3_size', 'bid3_size',
        'ask4_price', 'bid4_price',
        'ask4_size', 'bid4_size',
        'ask5_price', 'bid5_price',
        'ask5_size', 'bid5_size',
        'inserted_at',
    ])
    df.write_csv('../data/dump/contract_price_tick_fixed.csv')


def fix_contract_price_minute():
    df = pl.read_csv('../data/dump/contract_price_minute.csv')
    empty = pl.lit(None)
    df = df.with_columns([
        empty.alias('oi_open'),
        empty.alias('vol_open'),
        df['oi'].alias('oi_close'),
        df['vol'].alias('vol_close'),
    ]).select([
        'id', 'tradecode', 'dt',
        'open', 'high', 'low', 'close',
        'vol_open', 'vol_close',
        'oi_open', 'oi_close',
        'inserted_at', 'updated_at',
    ])
    df.write_csv('../data/dump/contract_price_minute_fixed.csv')


def fix_contract_price_daily():
    df = pl.read_csv('../data/dump/contract_price_daily.csv')
    empty = pl.lit(None)
    df = df.with_columns([
        empty.alias('oi_open'),
        empty.alias('vol_open'),
        df['oi'].alias('oi_close'),
        df['vol'].alias('vol_close'),
    ]).select([
        'id', 'tradecode', 'dt',
        'open', 'high', 'low', 'close',
        'vol_open', 'vol_close',
        'oi_open', 'oi_close', 'days_left',
        'inserted_at', 'updated_at',
    ])
    df.write_csv('../data/dump/contract_price_daily_fixed.csv')

if __name__ == '__main__':
    # fix_contract_price_tick()
    fix_contract_price_minute()
    fix_contract_price_daily()
