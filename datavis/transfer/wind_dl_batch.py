"""
wind_dl_batch.py
批量下载 wind 期权 1 分钟数据
"""
import pandas as pd
import datetime
import os
import click

from WindPy import w
import wind_dl

def dl_dt_range(spot: str, bgdt: datetime.date, eddt: datetime.date):
    bgdt_str = bgdt.strftime('%Y-%m-%d')
    eddt_str = eddt.strftime('%Y-%m-%d')
    dtdf = wind_dl.wind2df(w.tdays(bgdt_str, eddt_str, ""))
    dtdf['time'] = dtdf['']
    print(dtdf)
    if dtdf.shape[0] == 0:
        print("no trade days.")
        return
    dtdf['time'].to_csv(f"{spot}_trade_days.csv", index=False)
    for dtstp in dtdf['time']:
        dt = dtstp.to_pydatetime()
        if dt.weekday() >= 5:
            continue
        dt_str = dt.strftime('%Y-%m-%d')
        try:
            cnt = wind_dl.dl_data_bar(spot, dt_str)
        except Exception as e:
            print(f"error: {e}")
            continue

def main():
    # spot = "510500.SH"
    # for spot in ["510500.SH", "510050.SH", '510300.SH']:
    for spot in ['510300.SH']:
        bgdt = datetime.date(2025, 2, 18)
        eddt = datetime.date(2025, 4, 2)
        # bgdt = datetime.date(2024, 10, 1)
        # eddt = datetime.date(2024, 10, 31)
        dl_dt_range(spot, bgdt, eddt)

if __name__ == '__main__':
    main()