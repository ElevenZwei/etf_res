"""
下载期货日线数据
使用 WindPy 库从 Wind 数据服务下载期货合约列表和日线价格数据，并将其保存为 CSV 文件。
下载的期货合约列表包括合约代码、到期日、交易所等信息，日线价格数据包括开盘价、最高价、最低价和收盘价。
下载过程中会进行错误处理，尝试多次下载以应对可能的网络问题或 Wind 数据服务的临时故障。
下载完成后，生成的 CSV 文件可以用于后续的数据分析或导入到数据库中。
"""

from WindPy import w
from header import WindException, wind2df, wind_retry
import polars as pl
import datetime
import os

w.start()

""" Convert the raw data from Wind into a standardized format with columns: dt, tradecode, ohlc. """
def convert_bar(df):
    df = df.rename({ x: x.lower() for x in df.columns })
    df = df.rename({
        'time': 'dt',
        'name': 'tradecode',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
    })
    df = df.select(['tradecode', 'dt', 'open', 'high', 'low', 'close'])
    return df

def convert_contract_info(df):
    df = df.rename({ x: x.lower() for x in df.columns })
    if 'tradecode' in df.columns:
        df = df.drop('tradecode')
    df = df.rename({
        'code': 'tradecode',
        'last_trade_date': 'expiry',
    })
    df = df.select(['tradecode', 'expiry'])
    return df

def dl_future_contract_list(underlying: str, startdate: str, enddate: str):
    def dl_core():
        fut_list = w.wset("futurecc", f"startdate={startdate};enddate={enddate};wind_code={underlying}")
        fut_list = wind2df(fut_list)
        # wind2df should return a polars DataFrame, or convert here if not
        if not isinstance(fut_list, pl.DataFrame):
            fut_list = pl.DataFrame(fut_list)
        print(fut_list)
        return fut_list

    print(f"download future contract list for {underlying} from {startdate} to {enddate}")
    return wind_retry(dl_core)


def dl_future_daily_price(futcode: str, startdate: str, enddate: str):
    def dl_core():
        fut_data = w.wsd(futcode, "open,high,low,close", startdate, enddate, "Fill=Previous")
        fut_data = wind2df(fut_data)
        if not isinstance(fut_data, pl.DataFrame):
            fut_data = pl.DataFrame(fut_data)
        print(fut_data)
        fut_data = convert_bar(fut_data)

    print(f"download future daily price for {futcode} from {startdate} to {enddate}")
    return wind_retry(dl_core)


def dl_ad_future_contract_list():
    df = dl_future_contract_list('ad.shf', '2025-01-15', '2026-01-15')
    df = convert_contract_info(df)
    df = df.with_columns([
        pl.lit('SHFE').alias('exchange'),
        pl.lit(10).alias('lot_size'),
        df['tradecode'].alias('name'),
        pl.lit(0).alias('callput'),
        pl.lit('ad').alias('spotcode'),
        pl.lit(None).alias('chaincode'),
        pl.lit(None).alias('strike'),
        pl.lit(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')).alias('updated_at'),
    ])
    df = df.select(['tradecode', 'name', 'exchange', 'lot_size', 'callput',
                    'spotcode', 'chaincode', 'strike', 'expiry', 'updated_at'])
    df.write_csv(os.path.join(os.path.dirname(__file__), 'ad_future_contract_list.csv'))
    print(df)

def dl_contracts_daily_price():
    dfs = []
    df = pl.read_csv(os.path.join(os.path.dirname(__file__), 'data/contracts_to_dl.csv'))
    startdate = '2025-12-20'
    enddate = '2026-01-15'
    for row in df.iter_rows(named=True):
        wind_code = row['wind_code']
        fut_data = dl_future_daily_price(wind_code, startdate, enddate)
        fut_code = row['tradecode']
        out_path = os.path.join(os.path.dirname(__file__), f"data/{fut_code}_daily_price.csv")
        fut_data.write_csv(out_path)
        print(f"saved to {out_path}")
        dfs.append(fut_data)
    all_data = pl.concat(dfs)
    out_path = os.path.join(os.path.dirname(__file__), f"data/all_future_daily_price.csv")
    all_data.write_csv(out_path)

if __name__ == "__main__":
    dl_contracts_daily_price()
    # dl_ad_future_contract_list()
    # df = dl_future_daily_price('ag2601.shf', '2025-12-26', '2026-01-15')
    
