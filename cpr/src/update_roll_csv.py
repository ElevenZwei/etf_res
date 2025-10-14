# update roll_position.csv file from cpr.roll_merged table

import pandas as pd
from config import DATA_DIR, get_engine

engine = get_engine()

roll_args_ids = [1, 2]

for roll_args_id in roll_args_ids:
    df = pd.read_sql('''
            select * from cpr.roll_merged
            where roll_args_id = ''' + str(roll_args_id) + '''
            order by dt
            ''', engine)
    fname = f'roll_159915_{roll_args_id}.csv'
    df.to_csv(DATA_DIR / 'signal' / fname, index=False)
    print(f"read {len(df)} rows from cpr.roll_merged for id {roll_args_id} to {fname}.")
