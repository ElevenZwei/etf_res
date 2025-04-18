"""
这个文件要根据 train 和 validate 的结果来生成交易点位，并且标注属于哪一种触发情景。
"""

import click
import os
import pandas as pd
import numpy as np
from collections import deque

class TradeHelperReg1:
    """
    这是一个反向做趋势的策略。
    residual = real - pred
    交易的基本逻辑从 train 的 residual 分布开始，
    我们选择 train residual 的某个分位数，例如 0.2 和 0.8 来作为交易的触发点，
    当 validate residual 小于 0.2 分位数时，
    我们就认为是一个卖出开仓信号，在 real 变化成开仓时的 pred 的数字时平仓，或者在收盘时平仓。
    而当 validate residual 大于 0.8 分位数时，
    我们就认为是一个买入开仓信号，在 real 变化成开仓时的 pred 的数字时平仓，或者在收盘时平仓。
    """
    def __init__(self,
            long_enable: bool, short_enable: bool,
            long_trig: float, short_trig: float,
            mode_trend: bool, hold_to_close: bool, stop_loss: float):
        self.long_enable = long_enable
        self.short_enable = short_enable
        self.long_trig = long_trig
        self.short_trig = short_trig
        self.pos = 0
        self.entry_real = 0.0
        self.entry_pred = 0.0
        self.hold_to_close = hold_to_close
        self.stop_loss = stop_loss
        self.locked = False
        self.mode_mul = 1 if mode_trend else -1
    
    def next(self, dt, real, pred):
        if dt.hour == 9 and dt.minute < 35:
            self.locked = False
            return 0

        residual = real - pred
        if self.pos == 0 and not self.locked:
            if self.long_enable and self.mode_mul * (residual - self.long_trig) > 0:
                self.pos = 1
                self.entry_real = real
                self.entry_pred = pred
            elif self.short_enable and self.mode_mul * (residual - self.short_trig) < 0:
                self.pos = -1
                self.entry_real = real
                self.entry_pred = pred
        if not self.hold_to_close:
            if self.pos == 1 and self.mode_mul * (real - self.entry_pred) < 0:
                self.pos = 0
            if self.pos == -1 and self.mode_mul * (real - self.entry_pred) > 0:
                self.pos = 0
        
        if self.pos == 1 and (1 + real) / (1 + self.entry_real) < 1 - self.stop_loss:
            self.pos = 0
            self.locked = True
        if self.pos == -1 and (1 + real) / (1 + self.entry_real) > 1 + self.stop_loss:
            self.pos = 0
            self.locked = True

        if (dt.hour == 14 and dt.minute > 46) or dt.hour >= 15:
            self.locked = True
            self.pos = 0
        return self.pos


class TradeHelperReg2:
    """
    这个策略需要区分做空还是做多还是混合。
    尽量符合 周贺斌 原本的思路。
    residual = real - pred
    这个策略需要一直使用最新滚动的 1200 条 residual 来计算分位数。
    做回归只会归零，策略只能做趋势。
    """
    def __init__(self,
            long_enable: bool, short_enable: bool, window_size: int,
            negate_residual: bool,
            ):
        self.long_enable = long_enable
        self.short_enable = short_enable
        self.window_size = window_size
        self.residuals = deque(maxlen=window_size)
        self.residual_cache = []
        self.negate_residual = negate_residual
        self.pos = 0

        self.long_open_trig = 100
        self.short_open_trig = -100
        self.long_close_trig = -100
        self.short_close_trig = 100
        self.trig_cache = {
            'dt': [],
            'long_open': [],
            'short_open': [],
            'long_close': [],
            'short_close': [],
        }

        self.last_dt = None
    
    def set_init_residual(self, residual_arr):
        if self.negate_residual:
            residual_arr = -residual_arr
        self.residuals.extend(residual_arr)
    
    def next(self, dt, real, pred):
        residual = real - pred
        if self.negate_residual:
            residual = -residual
        self.residual_cache.append(residual)

        if self.last_dt is None or dt.date() > self.last_dt.date():
            self.residuals.extend(self.residual_cache)
            self.residual_cache = []
            self.long_open_trig = np.percentile(self.residuals, 80)
            self.short_open_trig = np.percentile(self.residuals, 20)
            self.long_close_trig = np.percentile(self.residuals, 0.1)
            self.short_close_trig = np.percentile(self.residuals, 50)
        self.last_dt = dt
        self.trig_cache['dt'].append(dt)
        self.trig_cache['long_open'].append(self.long_open_trig)
        self.trig_cache['short_open'].append(self.short_open_trig)
        self.trig_cache['long_close'].append(self.long_close_trig)
        self.trig_cache['short_close'].append(self.short_close_trig)

        if dt.hour == 9 and dt.minute < 32:
            self.pos = 0
            return 0

        if len(self.residuals) < self.window_size:
            return 0

        if self.pos == 0:
            if self.long_enable and residual > self.long_open_trig:
                self.pos = 1
            elif self.short_enable and residual < self.short_open_trig:
                self.pos = -1
        
        if self.pos == 1 and residual < self.long_close_trig:
            self.pos = 0
        if self.pos == -1 and residual > self.short_close_trig:
            self.pos = 0

        if (dt.hour == 14 and dt.minute > 56) or dt.hour >= 15:
            self.pos = 0
        return self.pos
    
    def get_trig_cache(self):
        df = pd.DataFrame(self.trig_cache)
        return df

class TradeHelperReg3:
    def __init__(self, long_enable: bool, short_enable: bool, window_size: int):
        self.long_enable = long_enable
        self.short_enable = short_enable
        self.window_size = window_size
        self.residuals = deque(maxlen=window_size)
        self.pos = 0
    
        self.last_residual_dt = None
        self.last_residual_opt_price = None
        self.residual_cache = []
        self.long_trig = 100.0
        self.short_trig = -100.0
    
    def set_init_residual(self, df):
        for a, b, c in zip(df['dt'], df['residual'], df['opt_price']):
            self.add_residual(a, b, c)
    
    def add_residual(self, dt, residual, opt_price):
        if (self.last_residual_dt is None
                or dt.date() > self.last_residual_dt.date()):
            # dump residual cache
            self.residuals.extend(self.residual_cache)
            self.residual_cache = []
            if len(self.residuals) >= self.window_size:
                self.long_open_trig = np.percentile(self.residuals, 80)
                self.short_open_trig = np.percentile(self.residuals, 20)
                self.long_close_trig = np.percentile(self.residuals, 0.1)
                self.short_close_trig = np.percentile(self.residuals, 50)
            # reset residual base price
            self.last_residual_dt = dt
            self.last_residual_opt_price = opt_price
        res = residual * self.last_residual_opt_price
        self.residual_cache.append(res)
        return res
    
    def next(self, dt, real, pred, opt_price):
        residual = real - pred
        residual = self.add_residual(dt, residual, opt_price)

        if dt.hour == 9 and dt.minute < 32:
            return 0
        if len(self.residuals) < self.window_size:
            return 0

        if self.pos == 0:
            if self.long_enable and residual > self.long_open_trig:
                self.pos = 1
            elif self.short_enable and residual < self.short_open_trig:
                self.pos = -1
        if self.pos == 1 and residual < self.long_close_trig:
            self.pos = 0
        if self.pos == -1 and residual > self.short_close_trig:
            self.pos = 0

        if (dt.hour == 14 and dt.minute > 46) or dt.hour >= 15:
            self.pos = 0
        return self.pos


def run_trade(train_df: pd.DataFrame, validate_df: pd.DataFrame):
    """
    运行交易策略，返回交易结果。
    :return: 交易结果数据框
    """
    reg1_long_quantile = 0.8
    reg1_short_quantile = 0.2
    daily_stop_loss = 0.04
    if train_df is None:
        long_trig = validate_df['residual'].quantile(reg1_long_quantile)
        short_trig = validate_df['residual'].quantile(reg1_short_quantile)
    else:
        long_trig = train_df['residual'].quantile(reg1_long_quantile)
        short_trig = train_df['residual'].quantile(reg1_short_quantile)
    print(f"Long Trigger: {long_trig}, Short Trigger: {short_trig}")
    df = validate_df.copy()

    reg1_trade_helper = TradeHelperReg1(
            long_enable=True, short_enable=True,
            long_trig=long_trig, short_trig=short_trig,
            mode_trend=True, hold_to_close=False, stop_loss=daily_stop_loss)
    df['reg1a_pos'] = df.apply(lambda row:
            reg1_trade_helper.next(row['dt'], row['real'], row['pred']), axis=1)

    reg1_trade_helper = TradeHelperReg1(
            long_enable=True, short_enable=False,
            long_trig=long_trig, short_trig=short_trig,
            mode_trend=True, hold_to_close=False, stop_loss=daily_stop_loss)
    df['reg1a_long_pos'] = df.apply(lambda row:
            reg1_trade_helper.next(row['dt'], row['real'], row['pred']), axis=1)

    reg1_trade_helper = TradeHelperReg1(
            long_enable=False, short_enable=True,
            long_trig=long_trig, short_trig=short_trig,
            mode_trend=True, hold_to_close=False, stop_loss=daily_stop_loss)
    df['reg1a_short_pos'] = df.apply(lambda row:
            reg1_trade_helper.next(row['dt'], row['real'], row['pred']), axis=1)

    # reg1_trade_helper = TradeHelperReg1(long_trig, short_trig,
    #         mode_trend=False, hold_to_close=False, stop_loss=0.05)
    # df['reg1c_pos'] = df.apply(lambda row:
    #         reg1_trade_helper.next(row['dt'], row['real'], row['pred']), axis=1)

    # reg1_trade_helper = TradeHelperReg1(long_trig, short_trig,
    #         mode_trend=False, hold_to_close=True, stop_loss=0.05)
    # df['reg1d_pos'] = df.apply(lambda row:
    #         reg1_trade_helper.next(row['dt'], row['real'], row['pred']), axis=1)

    wsize = 1200
    negate_residual = False
    reg2_trade_helper = TradeHelperReg2(long_enable=True, short_enable=False, window_size=wsize, negate_residual=negate_residual)
    if train_df is not None:
        reg2_trade_helper.set_init_residual(train_df['residual'].values[-wsize:])
    df['reg2a_pos'] = df.apply(lambda row:
            reg2_trade_helper.next(row['dt'], row['real'], row['pred']), axis=1)
    df2 = reg2_trade_helper.get_trig_cache().rename(mapper=lambda x: f'reg2a_{x}' if x != 'dt' else x, axis=1)
    df = pd.merge(df, df2, how='left', on='dt')
    reg2_trade_helper = TradeHelperReg2(long_enable=False, short_enable=True, window_size=wsize, negate_residual=negate_residual)
    if train_df is not None:
        reg2_trade_helper.set_init_residual(train_df['residual'].values[-wsize:])
    df['reg2b_pos'] = df.apply(lambda row:
            reg2_trade_helper.next(row['dt'], row['real'], row['pred']), axis=1)
    reg2_trade_helper = TradeHelperReg2(long_enable=True, short_enable=True, window_size=wsize, negate_residual=negate_residual)
    if train_df is not None:
        reg2_trade_helper.set_init_residual(train_df['residual'].values[-wsize:])
    df['reg2c_pos'] = df.apply(lambda row:
            reg2_trade_helper.next(row['dt'], row['real'], row['pred']), axis=1)

    # # reg3 
    # wsize = 1200
    # reg3_trade_helper = TradeHelperReg3(long_enable=True, short_enable=False, window_size=wsize)
    # if train_df is not None:
    #     reg3_trade_helper.set_init_residual(train_df[-wsize:])
    # df['reg3a_pos'] = df.apply(lambda row:
    #         reg3_trade_helper.next(row['dt'], row['real'], row['pred'], row['opt_price']), axis=1)
    # reg3_trade_helper = TradeHelperReg3(long_enable=False, short_enable=True, window_size=wsize)
    # if train_df is not None:
    #     reg3_trade_helper.set_init_residual(train_df[-wsize:])
    # df['reg3b_pos'] = df.apply(lambda row:
    #         reg3_trade_helper.next(row['dt'], row['real'], row['pred'], row['opt_price']), axis=1)
    # reg3_trade_helper = TradeHelperReg3(long_enable=True, short_enable=True, window_size=wsize)
    # if train_df is not None:
    #     reg3_trade_helper.set_init_residual(train_df[-wsize:])
    # df['reg3c_pos'] = df.apply(lambda row:
    #         reg3_trade_helper.next(row['dt'], row['real'], row['pred'], row['opt_price']), axis=1)

    return df

def trade_stat(trade_df: pd.DataFrame, pos_col: str):
    pos = trade_df[pos_col].values
    sig = pos - np.roll(pos, 1)
    sig[0] = 0
    trade_df['sig'] = sig
    # trade at next bar
    trade_df['open_p'] = trade_df['real'].shift(-1)
    trade_df = trade_df[trade_df['sig'] != 0].copy()
    if trade_df.shape[0] % 2 != 0:
        print('Error: the number of signals is not even, signal:', pos_col)
        trade_df = trade_df[:-1]
    trade_df['close_p'] = trade_df['open_p'].shift(-1)
    trade_df['close_dt'] = trade_df['dt'].shift(-1)
    trade_df = trade_df[::2]
    trade_df['label'] = pos_col
    trade_df['dir'] = trade_df['sig']

    trade_df['profit'] = ((1 + trade_df['close_p']) / (1 + trade_df['open_p']) - 1) * trade_df['sig']
    trade_df['profit_arith_sum'] = trade_df['profit'].cumsum()
    trade_df['profit_log'] = trade_df['profit'].apply(lambda x: np.log(1+x) if x > -1 else np.log(0.001))
    trade_df['profit_exp_sum'] = trade_df['profit_log'].cumsum().apply(np.exp)
    
    strategy = '_'.join(pos_col.split('_')[:-1])
    aux_cols = [x for x in trade_df.columns if x.startswith(strategy) and x != pos_col]
    trade_df = trade_df[['label', 'dt', 'close_dt', 'dir', 'open_p', 'close_p',
            'profit', 'profit_arith_sum', 'profit_log', 'profit_exp_sum',
            ] + aux_cols]
    print(trade_df)
    print('profit sum:', trade_df['profit'].sum())
    return trade_df

def data_dir(dt: str):
    """
    获取数据目录。
    :param dt: 日期
    :return: 数据目录
    """
    dt = pd.to_datetime(dt).date()
    return f'../output/{dt.strftime("%Y%m%d")}'

def read_csv(dt: str):
    dt = pd.to_datetime(dt).date()
    train_df = pd.read_csv(f'{data_dir(dt)}/pred_train.csv')
    validate_df = pd.read_csv(f'{data_dir(dt)}/pred_validate.csv')
    train_df['dt'] = pd.to_datetime(train_df['dt'])
    validate_df['dt'] = pd.to_datetime(validate_df['dt'])
    return train_df, validate_df

def main(dt: str):
    """
    主函数，解析命令行参数并调用其他函数。
    :param dt: 日期
    """
    dt = pd.to_datetime(dt).date()
    train_df, validate_df = read_csv(dt)
    
    # 运行交易策略
    # trade_train = run_trade(None, train_df)
    trade_validate = run_trade(train_df, validate_df)
    
    # 保存交易结果
    # trade_train.to_csv(f'{data_dir(dt)}/trade_train.csv', index=False)
    trade_validate.to_csv(f'{data_dir(dt)}/trade_validate.csv', index=False)

    # 计算交易统计
    pos_cols = [x for x in trade_validate.columns if x.endswith('_pos')]
    for col in pos_cols:
        prefix = col[:-4]
        # reg1_stat_train = trade_stat(trade_train, col)
        # reg1_stat_train.to_csv(f'{data_dir(dt)}/{prefix}_trades_train.csv', index=False)
        reg1_stat_val = trade_stat(trade_validate, col)
        reg1_stat_val.to_csv(f'{data_dir(dt)}/{prefix}_trades_validate.csv', index=False)


@click.command()
@click.option('-d', '--date', type=str, required=True,
              help="format is %Y%m%d")
def click_main(date: str):
    """
    主函数，解析命令行参数并调用其他函数。
    :param date: 日期
    """
    main(date)


if __name__ == '__main__':
    click_main()