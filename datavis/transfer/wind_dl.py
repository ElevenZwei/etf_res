from WindPy import w
import pandas as pd
import datetime
import click
import os

import wind_to_db as windtodb

w.start()

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
        df['name'] = wddata.Codes[0]
    return df

def dl_opt_names(spotcode: str, dtstr: str):
    print(f"get opt names on spot {spotcode} date {dtstr}")
    max_attempts = 3
    attempts = 0
    while attempts < max_attempts:
        try:
            opt_names = w.wset("optionchain",f"date={dtstr};us_code={spotcode};option_var=all;call_put=all")
            opt_names = wind2df(opt_names)
            break
        except WindException as we:
            attempts += 1
            if attempts == max_attempts:
                raise we
    return opt_names

def dl_data(spotcode: str, dtstr: str):
    opt_names = dl_opt_names(spotcode, dtstr)
    dt = datetime.datetime.strptime(dtstr, '%Y-%m-%d')
    max_attempts = 3
    for opt_code in [spotcode, *opt_names['option_code']]:
        print(f"get opt data, spot {spotcode} date {dtstr} opt {opt_code}")
        
        attempts = 0
        while attempts < max_attempts:
            try:
                opt_data = w.wst(opt_code, "ask1,asize1,bid1,bsize1,ask2,asize2,bid2,bsize2,last,oi,volume",
                                 f"{dtstr} 09:00:00", f"{dtstr} 11:00:51", "")
                opt_data = wind2df(opt_data)
                break
            except WindException as we:
                attempts += 1
                if attempts == max_attempts:
                    raise we

        opt_data = windtodb.convert_mdt_df(opt_data)
        opt_data.to_csv(f'../db/tick/mdt_{spotcode}_{opt_code}_{dt.strftime("%Y%m%d")}.csv', index=False)

def dl_data_bar(spotcode: str, dtstr: str):
    opt_names = dl_opt_names(spotcode, dtstr)
    if opt_names.shape[0] == 0:
        print(f"no opt names for {spotcode} on {dtstr}")
        return 0
    dt = datetime.datetime.strptime(dtstr, '%Y-%m-%d')
    dtstr1 = dt.strftime('%Y%m%d')
    mkdir = f'../db/bar/{spotcode}/{dtstr1}'
    if not os.path.exists(mkdir):
        os.makedirs(mkdir)
    opt_names.to_csv(f'{mkdir}/names_{spotcode}_{dtstr1}.csv', index=False)
    cnt = 0
    max_attempts = 3
    for opt_code in [spotcode, *opt_names['option_code']]:
        print(f"get opt bar, spot {spotcode} date {dtstr} opt {opt_code}")
        attempts = 0
        while attempts < max_attempts:
            try:
                opt_data = w.wsi(opt_code,
                        "open, high, low, close, volume, oi",
                        f"{dtstr} 09:00:00", f"{dtstr} 15:00:00",
                        f"BarSize=1")
                opt_data = wind2df(opt_data)
                break
            except WindException as we:
                attempts += 1
                if attempts == max_attempts:
                    raise we

        opt_data = windtodb.convert_mdt_bar(opt_data)
        opt_data_filtered = opt_data[opt_data['openp'] > 0]
        # 防止因为没有交易所以 OI 数据也错过了，只保存了一个空的文件。
        if opt_data_filtered.shape[0] == 0:
            opt_data_filtered = opt_data[:1]
        opt_data = opt_data_filtered
        if opt_code != spotcode:
            opt_info = opt_names[opt_names['option_code'] == opt_code].iloc[0]
            expiry_date = opt_info['expiredate']
            strike = opt_info['strike_price']
            opt_name = opt_info['option_name']
            callput = {'认购': 1, '认沽': -1}[opt_info['call_put']]
            opt_data['expirydate'] = expiry_date
            opt_data['strike'] = strike
            opt_data['name'] = opt_name
            opt_data['callput'] = callput
            opt_data['spotcode'] = spotcode
        else:
            opt_data['expirydate'] = None
            opt_data['strike'] = None
            opt_data['name'] = spotcode
            opt_data['callput'] = 0
            opt_data['spotcode'] = spotcode
        opt_data.to_csv(f'{mkdir}/bar_{spotcode}_{opt_code}_{dtstr1}.csv', index=False)
        cnt += 1
    print(f"get {cnt} opt bars for {spotcode} on {dtstr}")
    return cnt

def main(spots: list[str], dates: list[str], bar: bool):
    for spot in spots:
        for date in dates:
            if bar:
                dl_data_bar(spot, date)
            else:
                dl_data(spot, date)

@click.command()
@click.option('-s', '--spot', type=str)
@click.option('-d', '--date', type=str)
@click.option('--bar', is_flag=True)
def click_main(spot: str, date: str, bar: bool):
    if ',' in spot:
        spots = spot.split(',')
    else:
        spots = [spot]
    if ',' in date:
        dates = date.split(',')
    else:
        dates = [date]
    main(spots, dates, bar=bar)

if __name__ == '__main__':
    click_main()
    # for spotcode in ['510050.SH', '510300.SH', '159915.SZ', '510500.SH', '588000.SH']:
    # for spotcode in ['510500.SH', '588000.SH']:
    # for spotcode in ['510500.SH']:
    # for spotcode in ['510300.SH']: 
        # dl_data(spotcode, '2024-11-05')
        # dl_data(spotcode, '2024-11-06')
        # dl_data(spotcode, '2024-11-07')
        # dl_data(spotcode, '2024-11-08')
        # dl_data(spotcode, '2024-11-11')
        # dl_data(spotcode, '2024-11-12')