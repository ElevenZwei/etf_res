import pandas as pd
import glob
from backtest.config import DATA_DIR

fs = glob.glob((DATA_DIR / 'input/nifty_greeks*_n.csv').as_posix())
dfs = []
for file in fs:
    df = pd.read_csv(file)
    dfs.append(df)

out: pd.DataFrame = pd.concat(dfs)
out = out.sort_values(['dt', 'code'])
out.to_csv(DATA_DIR / 'input/nifty_greeks_combined.csv', index=False)

