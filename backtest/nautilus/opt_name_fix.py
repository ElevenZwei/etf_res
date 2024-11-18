# Nautilus Trader 不接受 ASCII 之外的 instrument name
# 所以这里只能做一个转换，最好数据库里也要做一个转换。

import pandas as pd

def convertTradecode(code: str):
    code = code.replace('创业板ETF', '159915')
    code = code.replace('购', 'C')
    code = code.replace('沽', 'P')
    code = code.replace('2024年', '')
    code = code.replace('3月', '2403M00')
    code = code.replace('4月', '2404M00')
    code = code.replace('5月', '2405M00')
    code = code.replace('6月', '2406M00')
    code = code.replace('7月', '2407M00')
    code = code.replace('8月', '2408M00')
    code = code.replace('9月', '2409M00')
    code = code.replace('10月', '2410M00')
    code = code.replace('11月', '2411M00')
    return code

df = pd.read_csv('../input/options_159915_minute_data.csv')
df['tradecode'] = df['tradecode'].apply(convertTradecode)
df.to_csv('../input/options_data.csv', index=False)
