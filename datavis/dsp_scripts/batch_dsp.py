"""
Dsp Batch
"""

import click
import date_dsp as dd
import pandas as pd
from multiprocessing import Pool, Lock

dl_lock = Lock()

def process_date(dt, spot, refresh, plot, signal, year, month, wide: bool):
    try:
        dt_str = dt.strftime('%Y%m%d')
        if refresh:
            with dl_lock:
                print(f'downloading {dt.date()}')
                dd.download_data(spot,
                        bg_str=dt_str, ed_str=dt_str,
                        year=year, month=month)
        print(f'calculating {dt.date()}')
        dd.date_dsp(spot,
                    bg_str=dt_str,
                    ed_str=dt_str,
                    refresh=False, plot=plot, signal=signal,
                    year=year, month=month,
                    show=False, save=True, wide=wide)
    except Exception as e:
        print(f'{dt.date()} failed, error: {e}')

@click.command()
@click.option('-s', '--spot', type=str, required=True, help="spot code: 159915 510050")
@click.option('-b', '--begin', type=str, help="format is %Y%m%d")
@click.option('-e', '--end', type=str, help="format is %Y%m%d")
@click.option('-r', '--refresh', is_flag=True, default=False, help="Download new data from database.")
@click.option('-p', '--plot', is_flag=True, default=False, help="Plot only, use existing data.")
@click.option('-g', '--signal', is_flag=True, default=False, help="Generate signal only, use existing data.")
@click.option('-y', '--year', type=int)
@click.option('-m', '--month', type=int)
@click.option('--wide', type=bool, default=False, help="use wide plot.")
def click_main(spot: str, begin: str, end: str,
               refresh: bool, plot: bool, signal: bool,
               year: int, month: int,
               wide: bool):
    # Intel i9 也只能跑两个并发
    max_concurrent_jobs = 1  # Set the maximum number of concurrent jobs
    with Pool(processes=max_concurrent_jobs) as pool:
        pool.starmap(process_date, [(dt, spot, refresh, plot, signal, year, month, wide)
                                    for dt in pd.date_range(begin, end)])
    
if __name__ == '__main__':
    click_main()
    pass
