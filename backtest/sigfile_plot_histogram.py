import pandas as pd
import plotly.express as px

# 这个是做多开仓的收益分布
df = pd.read_csv('input/ic_b_profit_2024.csv')
# 这个是做空开仓的收益分布
# df = pd.read_csv('input/ic_s_profit_2024.csv')
se_profits = df['bs_profit']

fig = px.histogram(se_profits, nbins=300, title='信号收益分布')
fig.show()