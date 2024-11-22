# 用  option 数据和历史成交记录合成一个分钟净值盈亏情况

import math
import tqdm
import pandas as pd
import pytz
from datetime import datetime, tzinfo
from collections import defaultdict

from backtest.config import DATA_DIR

def make_opt_pivot(df: pd.DataFrame):
    df = df[['dt', 'tradecode', 'closep']].copy()
    df = df.sort_values(['dt', 'tradecode'])
    df = df.drop_duplicates()
    df = df.rename(columns={'tradecode': 'code'})
    df['code'] = df['code'].apply(lambda x: 'OPT' + x)
    df['dt'] = pd.to_datetime(df['dt'])
    res = df.pivot(index='dt', columns='code', values='closep')
    return res

def make_order_df(df: pd.DataFrame):
    df['direction'] = df['direction'].map(
        {'BUY': 1, 'SELL': -1}
    )
    df['amount'] = df['amount'] * df['direction']
    df['code'] = df['code'].apply(lambda x: 'OPT' + x.rstrip('.sim'))
    df = df[['dt', 'code', 'amount', 'price']].copy()
    df['dt'] = pd.to_datetime(df['dt']).dt.tz_convert('Asia/Shanghai')
    df = df.set_index('dt')
    return df

# 这里用一种非常简单的手段计算盈亏，把所有的交易都视作开仓
class PosMan:
    def __init__(self):
        self.pos = []
    
    def open_pos(self, code, amount, price):
        self.pos.append({
            'code': code,
            'amount': amount,
            'price': price,
        })

    def calc_holdings_pnl(self, opt_line):
        res = 0
        for hold in self.pos:
            code = hold['code']
            opt_price = getattr(opt_line, code)
            if math.isnan(opt_price):
                opt_price = 0
            opt_pnl = hold['amount'] * (opt_price - hold['price'])
            # print(f'dt={opt_line.Index}, code={code}, price={opt_price}, open_price={hold['price']}, pnl={opt_pnl}')
            res += opt_pnl
        return res
    
    def compress_pos(self):
        pos = defaultdict(lambda: 0)
        for hold in self.pos:
            pos[hold['code']] += hold['amount']
        pos = { x: pos[x] for x in pos if pos[x] != 0}
        return pos
    
def calc_pnls(opt_df: pd.DataFrame, order_df: pd.DataFrame):
    pos = PosMan()
    pnls = []
    last_opt_dt = datetime(2024, 1, 1, tzinfo=pytz.timezone('Asia/Shanghai'))
    for opt_line in tqdm.tqdm(opt_df.itertuples(), total=opt_df.shape[0]):
        dt = opt_line.Index
        # print(opt_line)
        # print(dt)
        # print(order_df.index)
        orders = order_df[
                (order_df.index > last_opt_dt) & (order_df.index <= dt)]
        last_opt_dt = dt
        for order in orders.itertuples():
            pos.open_pos(order.code, order.amount, order.price)
        # if orders.shape[0] > 0:
            # print(f'orders on {dt}')
            # print(orders)
            # exit(1)
        pnl_now = pos.calc_holdings_pnl(opt_line)
        pnls.append({'dt': dt, 'pnl': pnl_now})
    print(pos.compress_pos())
    pnl_df = pd.DataFrame(pnls)
    return pnl_df

def main():
    opt_df = pd.read_csv(f'{DATA_DIR}/input/tl_greeks_159915_all_fixed.csv')
    opt_df = make_opt_pivot(opt_df)
    # print(opt_df)
    order_df = pd.read_csv(f'{DATA_DIR}/output/opt_bullsp_order_2_t.csv')
    order_df = make_order_df(order_df)

    """
    这个地方非常变态， tonglian的数据不是完整的 会缺少一些时间
    """
    order_dt = order_df.index.unique()
    opt_dt = opt_df.index.unique()
    order_only_dt = set(order_dt) - set(opt_dt)
    print(order_only_dt)

    # print(order_df)
    pnl_df = calc_pnls(opt_df, order_df)
    pnl_df = pnl_df.set_index('dt').resample('1d').last().reset_index()
    pnl_df = pnl_df[~pnl_df['pnl'].isna()]
    # print(pnl_df)
    pnl_df.to_csv(f'{DATA_DIR}/output/pnl_b5s1.csv', index=False, float_format='%.2f')


if __name__ == '__main__':
    main()
