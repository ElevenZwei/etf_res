"""
Dsp Batch
"""

import click
import date_dsp as dd
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading

dl_lock = threading.Lock()

def process_date(dt, spot, refresh, plot, year, month):
    try:
        if refresh:
            with dl_lock:
                print(f'downloading {dt.date()}')
                dd.download_data(spot, dt.strftime('%Y%m%d'), year, month)
        print(f'calculating {dt.date()}')
        dd.date_dsp(spot, dt.strftime('%Y%m%d'),
                    refresh=False, plot=plot,
                    year=year, month=month,
                    show=False, save=True)
    except Exception as e:
        print(f'{dt.date()} failed, error: {e}')
        # raise e

@click.command()
@click.option('-s', '--spot', type=str, required=True, help="spot code: 159915 510050")
@click.option('-b', '--begin', type=str, help="format is %Y%m%d")
@click.option('-e', '--end', type=str, help="format is %Y%m%d")
@click.option('-r', '--refresh', is_flag=True, default=False, help="Download new data from database.")
@click.option('-p', '--plot', is_flag=True, default=False, help="Plot only, use existing data.")
@click.option('-y', '--year', type=int)
@click.option('-m', '--month', type=int)
def click_main(spot: str, begin: str, end: str, refresh: bool, plot: bool, year: int, month: int):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_date, dt, spot, refresh, plot, year, month)
                for dt in pd.date_range(begin, end)]
        for future in futures:
            future.result()
            # try:
            #     future.result()
            # except Exception as e:
            #     print(f'error occurred on {future.result().date()}: {e}')
    
if __name__ == '__main__':
    click_main()
    pass
