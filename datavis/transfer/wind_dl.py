from WindPy import w
import pandas as pd
import datetime
import click

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

def dl_data(spotcode: str, dtstr: str):
    dt = datetime.datetime.strptime(dtstr, '%Y-%m-%d')
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
        
    for opt_code in [spotcode, *opt_names['option_code']]:
        print(f"get opt data, spot {spotcode} date {dtstr} opt {opt_code}")
        
        attempts = 0
        while attempts < max_attempts:
            try:
                opt_data = w.wst(opt_code, "ask1,asize1,bid1,bsize1,ask2,asize2,bid2,bsize2,last,oi,volume",
                                 f"{dtstr} 09:00:00", f"{dtstr} 15:18:51", "")
                opt_data = wind2df(opt_data)
                break
            except WindException as we:
                attempts += 1
                if attempts == max_attempts:
                    raise we

        opt_data = windtodb.convert_mdt_df(opt_data)
        opt_data.to_csv(f'../db/mdt_{spotcode}_{opt_code}_{dt.strftime("%Y%m%d")}.csv', index=False)

def main(spots: list[str], dates: list[str]):
    for spot in spots:
        for date in dates:
            dl_data(spot, date)

@click.command()
@click.option('-s', '--spot', type=str)
@click.option('-d', '--date', type=str)
def click_main(spot: str, date: str):
    if ',' in spot:
        spots = spot.split(',')
    else:
        spots = [spot]
    if ',' in date:
        dates = date.split(',')
    else:
        dates = [date]
    main(spots, dates)

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