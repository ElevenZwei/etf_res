# temporary script to merge OI files

import pandas as pd

from config import DATA_DIR

oi1 = pd.read_csv(DATA_DIR / 'fact' / 'oi_159915_20250101_20250709.csv')
oi2 = pd.read_csv(DATA_DIR / 'fact' / 'oi_merge' / 'oi_159915.csv')

def prep(df: pd.DataFrame) -> pd.DataFrame:
    df['dt'] = pd.to_datetime(df['dt']).dt.tz_convert('Asia/Shanghai')
    df = df.set_index('dt')
    df = df[['call_oi_sum', 'put_oi_sum', 'spot_price']]
    return df

oi1 = prep(oi1)
oi2 = prep(oi2)
oi = pd.concat([oi1, oi2])
oi = oi[~oi.index.duplicated(keep='last')]
oi.to_csv(DATA_DIR / 'fact' / 'oi_159915_full.csv')

