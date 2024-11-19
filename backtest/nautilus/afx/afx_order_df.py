# 整理生成的 order 表格数据

import click
import pandas as pd
from pathlib import Path

def process_file(df: pd.DataFrame):
    df = df[['ts_init', 'instrument_id', 'side', 'filled_qty', 'time_in_force', 'is_reduce_only', 'avg_px']].copy()
    # time_in_force GTC 表示 close
    df['is_close'] = df['time_in_force'].map({'GTC': True, 'FOK': False})
    df['is_near_expiry'] = df['is_reduce_only']
    df = df.rename(columns={
        'ts_init': 'dt',
        'instrument_id': 'code',
        'side': 'direction',
        'filled_qty': 'amount',
        'avg_px': 'price',
    })
    df = df.drop(columns=['time_in_force', 'is_reduce_only'])
    return df

def main(fpathstr: str):
    fpath: Path = Path(fpathstr)
    df = pd.read_csv(fpath)
    df = process_file(df)
    df.to_csv(f'../output/{fpath.stem}_t.csv')

@click.command()
@click.option('-f', '--file')
def click_main(file: str):
    main(file)

if __name__ == '__main__':
    click_main()
