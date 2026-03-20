"""
下载期货日线数据
使用 WindPy 库从 Wind 数据服务下载期货合约列表和日线价格数据，并将其保存为 CSV 文件。
下载的期货合约列表包括合约代码、到期日、交易所等信息，日线价格数据包括开盘价、最高价、最低价和收盘价。
下载过程中会进行错误处理，尝试多次下载以应对可能的网络问题或 Wind 数据服务的临时故障。
下载完成后，生成的 CSV 文件可以用于后续的数据分析或导入到数据库中。
"""

from WindPy import w
import pandas as pd
import datetime
import click
import os

w.start()

def convert_bar(df: pd.DataFrame):
    df = df.rename(columns={ x: x.lower() for x in df.columns })
    df = df.rename(columns={
        'time': 'dt',
        'name': 'tradecode',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
    })
    df = df[['tradecode', 'dt',
            'open', 'high', 'low', 'close',
    ]]
    return df


def convert_contract_info(df: pd.DataFrame):
    df = df.rename(columns={ x: x.lower() for x in df.columns })
    df = df.drop(columns=['tradecode'], errors='ignore')
    df = df.rename(columns={
        'code': 'tradecode',
        'last_trade_date': 'expiry',
    })
    df = df[['tradecode', 'expiry']]
    return df


class WindException(Exception):
    def __init__(self, msg, code):
        super().__init__(msg)
        self.code = code


def wind2df(wddata):
    if wddata.ErrorCode != 0:
        print(f"error code: {wddata.ErrorCode}")
        raise WindException("", wddata.ErrorCode)
    res = {}
    print("columns: ", wddata.Fields, ", out_len=", len(wddata.Data))
    if len(wddata.Times) > 1:
        res['time'] = wddata.Times
    for i in range(0, len(wddata.Fields)):
        res[wddata.Fields[i]] = wddata.Data[i]
    df = pd.DataFrame(res)
    if len(wddata.Codes) > 0:
        df['tradecode'] = wddata.Codes[0]
    return df

def dl_future_contract_list(underlying: str, startdate: str, enddate: str):
    print(f"download future contract list for {underlying} from {startdate} to {enddate}")
    max_attempts = 3
    attempts = 0
    while attempts < max_attempts:
        try:
            fut_list = w.wset("futurecc", f"startdate={startdate};enddate={enddate};wind_code={underlying}")
            fut_list = wind2df(fut_list)
            print(fut_list)
            break
        except WindException as we:
            attempts += 1
            if attempts == max_attempts:
                raise we
    return fut_list


def dl_future_daily_price(futcode: str, startdate: str, enddate: str):
    print(f"download future daily price for {futcode} from {startdate} to {enddate}")
    max_attempts = 3
    attempts = 0
    while attempts < max_attempts:
        try:
            fut_data = w.wsd(futcode, "open,high,low,close", startdate, enddate, "Fill=Previous")
            fut_data = wind2df(fut_data)
            print(fut_data)
            fut_data = convert_bar(fut_data)
            break
        except WindException as we:
            attempts += 1
            if attempts == max_attempts:
                raise we
    return fut_data


def dl_ad_future_contract_list():
    df = dl_future_contract_list('ad.shf', '2025-01-15', '2026-01-15')
    df = convert_contract_info(df)
    df['exchange'] = 'SHFE'
    df['lot_size'] = 10
    df['name'] = df['tradecode']
    df['callput'] = 0
    df['spotcode'] = 'ad'
    df['chaincode'] = None
    df['strike'] = None
    df['updated_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df = df[['tradecode', 'name', 'exchange', 'lot_size', 'callput',
             'spotcode', 'chaincode', 'strike', 'expiry', 'updated_at']]
    df.to_csv(os.path.join(os.path.dirname(__file__), 'ad_future_contract_list.csv'), index=False)
    print(df)

def dl_contracts_daily_price():
    dfs = []
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'data/contracts_to_dl.csv'))
    startdate = '2025-12-20'
    enddate = '2026-01-15'
    for idx, row in df.iterrows():
        wind_code = row['wind_code']
        fut_data = dl_future_daily_price(wind_code, startdate, enddate)
        fut_code = row['tradecode']
        out_path = os.path.join(os.path.dirname(__file__), f"data/{fut_code}_daily_price.csv")
        fut_data.to_csv(out_path, index=False)
        print(f"saved to {out_path}")
        dfs.append(fut_data)
    all_data = pd.concat(dfs, ignore_index=True)
    out_path = os.path.join(os.path.dirname(__file__), f"data/all_future_daily_price.csv")
    all_data.to_csv(out_path, index=False)

if __name__ == "__main__":
    dl_contracts_daily_price()
    # dl_ad_future_contract_list()
    # df = dl_future_daily_price('ag2601.shf', '2025-12-26', '2026-01-15')
    