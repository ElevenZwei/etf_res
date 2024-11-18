"""
把微信里面得到的中文信号转换成 db 需要的格式，
然后就可以从 db 里面提取到对应时间的 spot 价格数据。
"""
import pandas as pd

df = pd.read_csv('input/sig_159915.csv')
df.columns = ['dt', 'action']
df['price'] = 0
df['code'] = '159915.SZ'
df['action'] = df['action'].apply(lambda x: 1 if x == '买入' else -1)
df = df[['dt', 'code', 'price', 'action']]
df.to_csv('output/sig_159915_db.csv', index=False)