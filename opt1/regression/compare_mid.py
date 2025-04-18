import pandas as pd

INPUT_1 = '../input/sample_mid.csv'
INPUT_2 = '../input/生成中间数据/中间数据.csv'

df1 = pd.read_csv(INPUT_1)
df2 = pd.read_csv(INPUT_2)
df1['date'] = pd.to_datetime(df1['date']).dt.date
df2['date'] = pd.to_datetime(df2['date']).dt.date
df1 = df1[['date', 'option-ret-var']].drop_duplicates()
df2 = df2[['date', 'option-ret-var']].drop_duplicates()
df = df1.merge(df2, on='date', how='inner')
df.columns = ['date', 'option-ret-var_x', 'option-ret-var_y']
df.to_csv('../input/compare_mid.csv', index=False)
