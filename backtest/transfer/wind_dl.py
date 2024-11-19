# 它要获取每个品种最近到期的期权的全部数据。
# 这个文件按月下载所有期权的分钟级 Bar
# 我目前先只做 159915 和 510500

from datetime import date, timedelta
import calendar

from WindPy import w
import pandas as pd

w.start()

def wind2df(wddata):
    if wddata.ErrorCode != 0:
        print(f"error code: {wddata.ErrorCode}")
        return pd.DataFrame()
    res = {}
    print("columns: ", wddata.Fields, ", out_len=", len(wddata.Data))
    if len(wddata.Times) > 1:
        res['time'] = wddata.Times
    for i in range(0, len(wddata.Fields)):
        res[wddata.Fields[i]] = wddata.Data[i]
    df = pd.DataFrame(res)
    if len(wddata.Codes) > 0:
        df['name'] = wddata.Codes[0]
    return df

# date is yyyy-mm-dd
def dl_opt_info(spot: str, date: str):
    print(f"get opt names, spot={spot}, date={date}")
    opt_info = w.wset("optionchain",f"date={date};us_code={spot};option_var=all;call_put=all")
    opt_info = wind2df(opt_info)
    print(opt_info)
    opt_csv = opt_info.rename(columns={
        'option_code': 'code',
        'option_name': 'tradecode',
        'multiplier': 'contractunit',
        'us_code': 'spotcode',
        # callput
        'strike_price': 'strike',
        'last_tradedate': 'expirydate',
    })
    opt_csv['contractunit'] = opt_csv['contractunit'].astype('Int64')
    opt_csv['callput'] = opt_csv['call_put'].map({
        '认购': 1, '认沽': -1,
    })
    opt_csv = opt_csv[['code', 'tradecode', 'contractunit', 'spotcode',
            'callput', 'strike', 'expirydate']]
    opt_csv['insertdt'] = ''
    opt_csv.to_csv(f'db/ci_{spot}_{date}.csv', index=False)
    return opt_info

def dl_opt_data(spotcode: str, opt_code: str, from_date: str, to_date: str):
    print(f"get opt data, spot={spotcode}, opt={opt_code}, from={from_date}, to={to_date}")
    opt_data = w.wsi(opt_code, "high,low,open,close,volume,oi", f"{from_date} 09:00:00", f"{to_date} 15:30:00", "")
    opt_data = wind2df(opt_data)
    opt_data = opt_data.rename(columns={
        'time': 'dt',
        'open': 'openp',
        'close': 'closep',
        'high': 'highp',
        'low': 'lowp',
        'position': 'openinterest',
    })
    opt_data['code'] = opt_code
    opt_data = opt_data[opt_data['openp'] > 0]
    opt_data['openinterest'] = opt_data['openinterest'].astype('Int64')
    opt_data['volume'] = opt_data['volume'].astype('Int64')
    opt_data = opt_data[['dt', 'code',
            'openp', 'highp', 'lowp', 'closep',
            'volume', 'openinterest']]
    # remove empty lines.
    opt_data.to_csv(f'db/md_{spotcode}_{opt_code}_{from_date}_{to_date}.csv', index=False)
    return opt_data

def get_last_day(year, month) -> date:
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day)

def dl_year_data(spotcode: str, year: str):
    for month in range(1, 13):
    # for month in range(1, 2):
        first_day_str = date(year, month, 1).strftime('%Y-%m-%d')
        last_day_str = get_last_day(year, month).strftime('%Y-%m-%d')
        dl_opt_data(spotcode, spotcode, first_day_str, last_day_str)
        near_expiry_date = date(year, month, 20).strftime('%Y-%m-%d')
        opt_names = dl_opt_info(spotcode, near_expiry_date)['option_code']
        for opt_code in opt_names:
            dl_opt_data(spotcode, opt_code, first_day_str, last_day_str)

def main():
    # dl_year_data('159915.SZ', 2019)
    # dl_year_data('159915.SZ', 2020)
    # dl_year_data('159915.SZ', 2021)
    # dl_year_data('159915.SZ', 2022)
    # dl_year_data('159915.SZ', 2023)
    dl_year_data('159915.SZ', 2024)
    dl_year_data('510500.SH', 2024)
    
if __name__ == '__main__':
    main()
