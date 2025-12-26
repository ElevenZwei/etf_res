"""
Download minute-level stock data for the year 2023-2024
from cpr.market_minute table
and save it as a CSV file.
"""

import sys
from pathlib import Path
from datetime import time
import polars as pl
import sqlalchemy as sa

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import DATA_DIR, get_engine

def fetch_stock_csv():
    query = sa.text('''
        select dt, code, openp, closep from "cpr"."market_minute"
        where code = '159915'
        and dt >= '2023-01-01'
        and dt < '2025-01-01'
    ''')
    with get_engine().connect() as conn:
        df = pl.read_database(query, conn)
    df = df.with_columns(
        pl.col("dt").dt.convert_time_zone("Asia/Shanghai").alias("dt"),
    )
    df = df.filter(
            pl.col("dt").dt.time() >= time(9, 30)
    )
    return df

if __name__ == '__main__':
    df = fetch_stock_csv()
    df.write_csv(DATA_DIR / 'fact' / 'spot_159915_2023_2024.csv')

