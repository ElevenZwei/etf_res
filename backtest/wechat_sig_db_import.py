# 把陈诚给出的交易信号转成 db 需要的格式
import pandas as pd

df = pd.read_csv('input/ICLog.csv')
df['dt'] = df['time']
df['price'] = df['data_price']
df['action'] = df['buy_or_sell'].map({'买入': 1, '卖出': -1})
df['code'] = 'IC'

df = df[['dt', 'code', 'price', 'action']]
df.to_csv('db/ic_db.csv', index=False)
print(df)
