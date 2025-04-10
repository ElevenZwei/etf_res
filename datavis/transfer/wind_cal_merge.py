import glob
import os
import pandas as pd

def main(spot: str):
    fs = glob.glob(f'../db/oi/oi_sum_{spot}_*.csv')
    dfs = [pd.read_csv(f) for f in fs]
    df = pd.concat(dfs, ignore_index=True)
    df.to_csv(f'../db/oi2/{spot}.csv', index=False)

if __name__ == '__main__':
    main('510050.SH')
    main('510500.SH')
    main('510300.SH')
