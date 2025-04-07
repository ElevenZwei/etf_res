"""
Dsp Batch
"""

import click
import date_dsp as dd
import pandas as pd

def process_date(dt, spot, refresh, plot, signal, year, month, wide: bool, minute_bar: bool):
    try:
        dt_str = dt.strftime('%Y%m%d')
        if refresh:
            print(f'downloading {dt.date()}')
            dd.download_data(spot,
                    bg_str=dt_str, ed_str=dt_str,
                    year=year, month=month, minute_bar=minute_bar)
        print(f'calculating {dt.date()}')
        dd.date_dsp(spot,
                    bg_str=dt_str,
                    ed_str=dt_str,
                    refresh=False, plot=plot, signal=signal,
                    year=year, month=month, minute_bar=minute_bar,
                    show=False, save=True, wide=wide)
    except Exception as e:
        print(f'{dt.date()} failed, error: {e}')
        # print error message into a log file
        with open('error_log.txt', 'a') as f:
            f.write(f'{dt.date()} failed, error: {e}\n')

@click.command()
@click.option('-s', '--spot', type=str, required=True, help="spot code: 159915 510050")
@click.option('-b', '--begin', type=str, help="format is %Y%m%d")
@click.option('-e', '--end', type=str, help="format is %Y%m%d")
@click.option('-r', '--refresh', is_flag=True, default=False, help="Download new data from database.")
@click.option('-p', '--plot', is_flag=True, default=False, help="Plot only, use existing data.")
@click.option('-g', '--signal', is_flag=True, default=False, help="Generate signal only, use existing data.")
@click.option('-y', '--year', type=int)
@click.option('-m', '--month', type=int)
@click.option('--bar', '--minute-bar', is_flag=True, help="download from minute bar table.")
@click.option('--wide', type=bool, default=False, help="use wide plot.")
def click_main(spot: str, begin: str, end: str,
               refresh: bool, plot: bool, signal: bool,
               year: int, month: int,
               wide: bool, bar: bool):
    [process_date(dt, spot, refresh, plot, signal, year, month, wide, bar)
            for dt in pd.date_range(begin, end)
            if dt.weekday() < 5]
    
if __name__ == '__main__':
    click_main()
    pass
