"""
这里写一个比较简单的脚本，运行一天的回测并且根据现有的 cpr.roll_result 组合 cpr.roll_merged 表格。

1. Load Data 这个和 weekly_update.py 里面的 load_data 函数类似，把指定时间段内的现有数据都加载到 cpr 数据库里面去。
2. Run Backtest 这个和 weekly_update.py 里面的 backtest_data 函数类似，把指定时间段内的数据都跑一遍回测，生成 cpr.backtest_result 表格。
3. Merge Roll Results 这个和 weekly_update.py 里面的 roll_data 函数类似，把现有的 cpr.roll_result 表格里面的数据都合并成 cpr.roll_merged 表格。

"""

import click
from weekly_update import load_data, backtest_data, roll_data
from datetime import date, datetime

def main(spot: str, dt_bg: date, dt_ed: date):
    load_data(spot, dt_bg, dt_ed)
    backtest_data(spot, dt_bg, dt_ed)
    roll_data(spot, dt_bg, dt_ed,
              with_roll_next=False,
              with_roll_export=False)

@click.command()
@click.option('-s', '--spot', type=str, required=True, help='spot identifier, e.g. 159915')
@click.option('-b', '--date-bg', type=click.DateTime(formats=["%Y-%m-%d"]), required=True, help='Start date (YYYY-MM-DD)')
@click.option('-e', '--date-ed', type=click.DateTime(formats=["%Y-%m-%d"]), required=False, help='End date (YYYY-MM-DD)')
def click_main(spot: str, date_bg: datetime, date_ed: datetime):
    if date_ed is None:
        date_ed = datetime.now()
    main(spot, date_bg.date(), date_ed.date())


if __name__ == "__main__":
    click_main()

