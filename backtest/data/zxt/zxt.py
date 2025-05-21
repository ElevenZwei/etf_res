# 这个代码里面对输入数据做一些简单的探查

import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet('./pc_final_pos.parquet', engine='pyarrow')
print(df.columns)
print(df.head())
# print(df.info())
print(df.describe())

df['159915'].plot()
plt.show()