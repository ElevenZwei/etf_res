# update roll_position.csv file from cpr.roll_merged table

import pandas as pd
from config import DATA_DIR, get_engine

engine = get_engine()

df = pd.read_sql('select * from cpr.roll_merged', engine)
print(f"read {len(df)} rows from cpr.roll_merged")
df.to_csv(DATA_DIR / 'signal' / 'roll_position.csv', index=False)
