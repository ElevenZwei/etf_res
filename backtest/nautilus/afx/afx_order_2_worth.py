# 用  option 数据和历史成交记录合成一个分钟净值盈亏情况

import click
import math
import tqdm
import pandas as pd
import pytz
from datetime import datetime, tzinfo
from collections import defaultdict

from backtest.config import DATA_DIR

def make_opt_pivot(df: pd.DataFrame):
    if 'tradecode' not in df.columns:
        df['tradecode'] = df['code']
    if 'closep' not in df.columns:
        df['closep'] = df['price']
    df = df[['dt', 'tradecode', 'closep']].copy()
    df['dt'] = pd.to_datetime(df['dt'])
    df = df.sort_values(['dt', 'tradecode'])
    df = df.drop_duplicates()
    df = df.rename(columns={'tradecode': 'code'})
    df['code'] = df['code'].apply(lambda x:
            'OPT' + x.rstrip('.SZ').replace('-', '_'))
    # df['dt'] = pd.to_datetime(df['dt'])
    res = df.pivot(index='dt', columns='code', values='closep')
    print(df)
    df = df.ffill()
    return res

def make_order_df(df: pd.DataFrame):
    df['direction'] = df['direction'].map(
        {'BUY': 1, 'SELL': -1}
    )
    df['amount'] = df['amount'] * df['direction']
    df['code'] = df['code'].apply(lambda x:
            'OPT' + x.rstrip('.sim').rstrip('.SZ').replace('-', '_'))
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

def main(suffix: str, principal: float = 1000000.0):
    # order_df = pd.read_csv(f'{DATA_DIR}/output/opt_bullsp_order_5_t.csv')
    order_df = pd.read_csv(f'{DATA_DIR}/output/opt_order_{suffix}_t.csv')
    order_df = make_order_df(order_df)

    # opt_df = pd.read_csv(f'{DATA_DIR}/input/tl_greeks_159915_all_fixed.csv')
    opt_df = pd.read_csv(f'{DATA_DIR}/input/opt_159915_2025_greeks.csv')
    # opt_df = pd.read_csv(f'{DATA_DIR}/input/oi_spot_159915.csv')
    # opt_df = pd.read_csv(f'{DATA_DIR}/input/spot_159915_2025_dsp.csv')
    # opt_df = pd.read_csv(f'{DATA_DIR}/input/nifty_greeks_combined.csv')
    opt_df['code'] = opt_df['code'].astype('Int64').astype(str)
    opt_df = make_opt_pivot(opt_df)
    print(opt_df)
    # opt_df.to_csv('opt_df.csv')

    """
    这个地方非常变态， tonglian 的数据不是完整的 会缺少一些时间
    """
    order_dt = order_df.index.unique()
    opt_dt = opt_df.index.unique()
    order_only_dt = set(order_dt) - set(opt_dt)
    # print(order_only_dt)

    # print(order_df)
    pnl_df = calc_pnls(opt_df, order_df)
    pnl_df = pnl_df.set_index('dt').resample('1d').last().reset_index()
    pnl_df = pnl_df[~pnl_df['pnl'].isna()]
    pnl_df['dt'] = pnl_df['dt'].dt.date
    pnl_df['ratio'] = pnl_df['pnl'] / principal
    pnl_df['ratio_diff'] = pnl_df['ratio'].diff().fillna(0)
    # print(pnl_df)
    pnl_df.to_csv(f'{DATA_DIR}/output/pnl_{suffix}.csv', index=False, float_format='%.2f')

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-s', '--suffix', type=str, default='', help='suffix for output file')
@click.option('-p', '--principal', type=float, default=1000000.0, help='initial principal for pnl calculation')
def click_main(suffix: str, principal: float):
    main(suffix, principal)

if __name__ == '__main__':
    click_main()
