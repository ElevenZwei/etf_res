"""
这个文件要根据 train 和 validate 的结果来生成交易点位，并且标注属于哪一种触发情景。
交易的基本逻辑从 train 的 residual 分布开始，
我们选择 train residual 的某个分位数，例如 0.3 和 0.7 来作为交易的触发点，
当 validate residual 小于 0.3 分位数时，
我们就认为是一个卖出开仓信号，在 real 变化成开仓时的 pred 的数字时平仓，或者在收盘时平仓。
而当 validate residual 大于 0.7 分位数时，
我们就认为是一个买入开仓信号，在 real 变化成开仓时的 pred 的数字时平仓，或者在收盘时平仓。
"""

import click
import os
import pandas as pd
import numpy as np

class TradeHelper:
    def __init__(self, long_trig: float, short_trig: float):
        self.long_trig = long_trig
        self.short_trig = short_trig
        self.pos = 0
        self.entry_pred = 0.0
    
    def next(self, dt, real, pred):
        if dt.hour == 9 and dt.minute < 35:
            return 0

        residual = real - pred
        if self.pos == 0:
            if residual > self.long_trig:
                self.pos = 1
                self.entry_pred = pred
            elif residual < self.short_trig:
                self.pos = -1
                self.entry_pred = pred
        elif self.pos == 1 and real < self.entry_pred:
            self.pos = 0
        elif self.pos == -1 and real > self.entry_pred:
            self.pos = 0

        if (dt.hour == 14 and dt.minute > 46) or dt.hour >= 15:
            self.pos = 0
        return self.pos


def run_trade(df: pd.DataFrame, long_trig: float, short_trig: float):
    """
    运行交易策略，返回交易结果。
    :param df: 数据框
    :param long_trig: 买入触发点
    :param short_trig: 卖出触发点
    :return: 交易结果数据框
    """
    trade_helper = TradeHelper(long_trig, short_trig)
    df['reg1'] = df.apply(lambda row:
                         trade_helper.next(row['dt'], row['real'], row['pred']), axis=1)
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
    trade_df['profit'] = (trade_df['close_p'] - trade_df['open_p']) * trade_df['sig']
    trade_df = trade_df[['label', 'dt', 'close_dt', 'dir', 'open_p', 'close_p', 'profit']]
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
    
    # 计算分位数
    long_trig = train_df['residual'].quantile(0.80)
    short_trig = train_df['residual'].quantile(0.20)
    print(f"Long Trigger: {long_trig}, Short Trigger: {short_trig}")
    # 运行交易策略
    trade_train = run_trade(train_df, long_trig, short_trig)
    trade_validate = run_trade(validate_df, long_trig, short_trig)
    
    # 保存交易结果
    trade_train.to_csv(f'{data_dir(dt)}/trade_train.csv', index=False)
    trade_validate.to_csv(f'{data_dir(dt)}/trade_validate.csv', index=False)

    # 计算交易统计
    reg1_stat_train = trade_stat(trade_train, 'reg1')
    reg1_stat_train.to_csv(f'{data_dir(dt)}/reg1_trades_train.csv', index=False)
    reg1_stat_val = trade_stat(trade_validate, 'reg1')
    reg1_stat_val.to_csv(f'{data_dir(dt)}/reg1_trades_validate.csv', index=False)


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