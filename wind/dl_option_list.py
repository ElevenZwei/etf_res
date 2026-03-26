"""
下载期权品种列表数据
提供一个函数，输入 ETF 或者期货品种和某一个日期，
输出这一天可以交易的期权品种列表，整理成一个规范的格式。
这个格式可以在脚本之间传递数据，也可以上传到数据库里。
"""

from WindPy import w
from header import WindException, wind2df, wind_retry
import polars as pl
import datetime
import os

w.start()

def dl_opt_names(spotcode: str, dtstr: str) -> pl.DataFrame:
    def dl_core():
        opt_names = w.wset("optionchain",f"date={dtstr};us_code={spotcode};option_var=all;call_put=all")
        opt_names = wind2df(opt_names)

    print(f"get opt names on spot {spotcode} date {dtstr}")
    return wind_retry(dl_core)


if __name__ == '__main__':
    print(dl_opt_names('159915.SZ', '2026-03-26'))
