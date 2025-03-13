"""
这个在服务器上面定期运行然后把交易点位储存到数据库里面。
"""

import click
import datetime
from typing import Optional

import s0_md_query as s0
import s5_oi as s5
import s7_oi_stats as s7
import s9_trade_signal as s9
from dsp_config import DATA_DIR, PG_DB_CONF, gen_suffix

def func(spot: str, dt: str, year: int, month: int):
    wide = False
    print(f'download {spot} {dt}')
    suffix = s0.auto_dl(spot, year=year, month=month, bg_str=dt, ed_str=dt)
    s5.calc_intersect(spot, suffix, wide=wide)
    sufs5 = suffix + '_s5'
    s7.calc_stats_csv(spot, sufs5, wide=wide)
    s9.calc_signal_csv(spot, sufs5, wide=wide)

def main():
    dt = datetime.datetime.now().date()
    dt_str = dt.strftime('%Y%m%d')
    func('159915', dt_str, year=None, month=None)

@click.command()
@click.option('-d', '--date', type=str, default=None)
def click_main(date: Optional[str]):
    if date is None:
        main()
    else:
        func('159915', date, year=None, month=None)

if __name__ == '__main__':
    click_main()
