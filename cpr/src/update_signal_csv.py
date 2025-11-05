# update roll_159915_<roll_args_id>.csv file from cpr.roll_merged table
# update stock_399006_avg.csv file from cpr.stock_signal table

import pandas as pd
from config import DATA_DIR, get_engine

engine = get_engine()

roll_args_ids = [1, 2]

def update_roll_csv():
    for roll_args_id in roll_args_ids:
        df = pd.read_sql('''
                select * from cpr.roll_merged
                where roll_args_id = ''' + str(roll_args_id) + '''
                order by dt
                ''', engine)
        fname = f'roll_159915_{roll_args_id}.csv'
        df.to_csv(DATA_DIR / 'signal' / fname, index=False)
        print(f"read {len(df)} rows from cpr.roll_merged for id {roll_args_id} to {fname}.")

def update_stock_csv():
    df = pd.read_sql("""
            with input as (
                select acname, insert_time as dt, ps as position
                from cpr.stock_signal
                where product = '399006'
                    and acname in (
                      'pyelf_CybWoOacVsw_sif_1_1',
                      'pyelf_CybWoOacVswRR_sif_1_1',
                      'pyelf_CybWoOacVswRm_sif_1_1',
                      'pyelf_CybWoOacVswRmRR_sif_1_1'
                    )
                    and insert_time > '2025-09-30'
            )
            select dt, avg(position) as position from input
            group by dt
            order by dt;""", engine)
    fname = f'stock_399006_avg.csv'
    df.to_csv(DATA_DIR / 'signal' / fname, index=False)
    print(f"read {len(df)} rows from cpr.stock_signal.")

if __name__ == '__main__':
    update_roll_csv()
    update_stock_csv()



