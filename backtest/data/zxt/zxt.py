# 这个代码里面对输入数据做一些简单的探查

import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet('./order_signal.parquet', engine='pyarrow')
# change index name to dt
df = df.rename_axis('dt')
df = df[df.index >= '2023-01-01']
print(df.columns)
print(df.head())
print(df.tail())
# print(df.info())
print(df.describe())
df.to_csv('./pc_final_pos.csv')
for col in df.columns:
    if col.startswith('399'):
        df[col] = df[col] * 2 - 1  # convert to -1, 0, 1

# df['399006'].plot()
df['399006'].cumsum().plot()
df['399102'].cumsum().plot()
df['399673'].cumsum().plot()
plt.show()