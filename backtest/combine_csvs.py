import pandas as pd
import glob

fs = glob.glob('./db/md_159915.SZ_159915*.csv')
dfs = []
for file in fs:
    df = pd.read_csv(file)
    dfs.append(df)

out = pd.concat(dfs)
out.to_csv('db/159915_all.csv', index=False)