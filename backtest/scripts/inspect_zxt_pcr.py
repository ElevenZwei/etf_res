"""
这个脚本的作用是检查 ZXT PCR 处理之后的数据是否正确。
读取 mask_zxt_pcr.py 生成的 zxt_mask_position.parquet
"""

import pandas as pd
import matplotlib.pyplot as plt
from backtest.config import DATA_DIR

df = pd.read_parquet(f'{DATA_DIR}/input/zxt_mask_position.parquet', engine='pyarrow')
df = df.rename_axis('dt')
print(df)
print(df.describe())

df['stock_position'].cumsum().plot()
plt.show()