# 在 159915 external signal buy option 回测的时候看到一个特殊的现象
# 用当前资产乘以一定比例买入的效果最糟糕，
# 固定手数的交易收益好一些，
# 固定买多少钱的期权的投资效果最好。

# 这种现象是普遍存在的吗，还是只在这一个策略回测的过程里面有？
# 可能是这个回测里面有，因为这个信号预测 ETF 方向的时候有收益上升之后容易回归的特点。
# 如果普遍存在的话这是不是说明期权的时间价值流失速度太快了，其实到期末的时候随便搞买入都可以赚到很多？
# 无论怎样我先要对比一下这两组交易数据的差异，他们的品种和交易时间应该完全一致，只有交易量上面的差别。

# 我把比较对象放在 backtest 2 和 backtest 3 这两组的对比上。
# 结论是超额收益是慢慢累积的，没有哪个时间特别赚钱。
# 最明显的差异是每到期末它就会开出五六十手的买方平值期权。
# 比如在到期的前两天，-2d 和 -1d 的时候多买入，在0d的时候退出
# 总体上它是越到期交易量就越来越大。
# 为了分析清楚这种受益的来源，我可以用几个不同的方式验证，
# 比如说看看陈诚提供的信号是不是在到期日附近的正确率非常高。
# 好像也没有，都差不太多，从日均收益上没有明显的偏向。
# 比如说看看用 oi 信号加上这种交易策略是不是有一样的变化情况。
# 我预期可能是平值期权在月初的时间价值被高估了，在月末的时间价值被低估了，导致了这种现象。

# 考虑到这个超额收益是慢慢累积的，如果我们就用一个普通的信号，
# 然后做这两种交易策略之间的差价的话，是否可以慢慢积累财富？

# 我发现 backtest 2 的某些交易触发了单笔 1m 的封顶问题，所以 backtest 2 实际上的交易量和计算的不一样
# 我分成了带有封顶的和不带封顶的两个版本 backtest 2 和 backtest 4 延续不带封顶的测试
# 不带封顶的话，backtest4 的盈亏应该和 backtest1 差不多，
# 然后 backtest2 的意外告诉我一开始少买，临近期末带封顶多买可能会很有好处。


import pandas as pd

# Orders 之间的差异对比

def prepare_order_csv(fpath):
    df2 = pd.read_csv(fpath)
    # df3 = pd.read_csv('./output/opt_order_3.csv')
    df2.loc[:, 'dt'] = pd.to_datetime(df2['ts_init'])
    df2 = df2[['dt', 'instrument_id', 'side', 'is_reduce_only', 'quantity', 'avg_px']]
    df2['dir'] = df2['side'].apply(lambda x: 1 if x == 'BUY' else -1)
    df2['offset'] = df2['is_reduce_only'].apply(lambda x: 'close' if x else 'open')
    df2['amount'] = df2['dir'] * df2['quantity']
    df2 = df2.rename(columns={
        'instrument_id': 'contract',
        'avg_px': 'price',
    })
    return df2

def orders_have_same_dt_inst(df1, df2):
    dt_diff_rows = df1[df1['dt'] != df2['dt']]
    inst_diff_rows = df1[df1['contract'] != df2['contract']]
    if dt_diff_rows.empty and inst_diff_rows.empty:
        return True
    print(f"dt diff rows:\n{repr(dt_diff_rows)}")
    print(f"inst diff rows:\n{repr(inst_diff_rows)}")
    return False

def diff_orders_vol(df1, df2, multiplier):
    res = df1.copy()
    res['amount'] = df1['amount'] * multiplier - df2['amount']
    res['hand'] = res['amount'] // 10000
    res = res[['dt', 'contract', 'offset', 'hand', 'price']]
    # 只记录开仓时候的差异，去掉平仓的。
    # res = res.loc[~((res['contract'] == res['contract'].shift(1)) & (res['hand'] == -res['hand'].shift(1)))]
    res = res.loc[res['offset'] == 'open']
    return res

def compare_order(suffix_1, suffix_2, multiplier):
    dfa = prepare_order_csv(f'./output/opt_order_{suffix_1}.csv')
    dfb = prepare_order_csv(f'./output/opt_order_{suffix_2}.csv')
    if not orders_have_same_dt_inst(dfa, dfb):
        return
    diff_df = (diff_orders_vol(dfa, dfb, multiplier))
    diff_df = diff_df[['dt', 'hand', 'price']]
    diff_df.to_csv(f'./output/order_diff_raw_{suffix_1}_{suffix_2}.csv')
    # daily mean
    diff_df = diff_df.set_index('dt').resample('1d').mean()
    diff_df = diff_df.loc[~diff_df['hand'].isna()]
    diff_df.to_csv(f'./output/order_diff_{suffix_1}_{suffix_2}.csv')
    

# Account 之间的差异对比

def prepare_account_csv(fpath):
    df = pd.read_csv(fpath)
    return df

def diff_net_worth(df1, df2):
    assert(df1.shape == df2.shape)
    res = df1.copy()
    res['diff'] = df1['net'] - df2['net']
    res = res[['dt', 'diff']]
    return res

def compare_net_worth():
    df2 = prepare_account_csv('./output/buy_net_worth_fixed_2.csv')
    df3 = prepare_account_csv('./output/buy_net_worth_fixed_3.csv')
    diff_df = diff_net_worth(df2, df3)
    diff_df.to_csv('./output/account_diff_2_3.csv', index=False)
    
if __name__ == '__main__':
    compare_order('1', '2', 1)
    # compare_net_worth()

    

