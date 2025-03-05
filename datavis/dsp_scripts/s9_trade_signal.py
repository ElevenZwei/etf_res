"""
这个脚本需要拓展 s7 里面的交易信号。
尝试各种不同的交易信号，保存到 csv  文件里。
"""

import click
import pandas as pd
import numpy as np

from dsp_config import DATA_DIR, gen_wide_suffix
from helpers import OpenCloseHelper, DiffHelper

def getTradeCols(wide: bool):
    if wide:
        return {
            'ts_col': 'oi_cp_dirstd_ts_600',
            'sigma_col': 'oi_cp_dirstd_sigma_0.4',
            'spot_col': 'spot_price',
        }
    else:
        return {
            'ts_col': 'oi_cp_dirstd_ts_600',
            'sigma_col': 'oi_cp_dirstd_sigma_0.15',
            'spot_col': 'spot_price',
        }

class TsOpenHelper:
    def __init__(self, ts_open, ts_close):
        self.helper = OpenCloseHelper(ts_open, ts_close, -ts_open, -ts_close)
    
    def next(self, ts, sigma, spot):
        return self.helper.next(ts)


class SigmaOpenHelper:
    def __init__(self, sigma_open, sigma_close):
        self.helper = OpenCloseHelper(sigma_open, sigma_close, -sigma_open, -sigma_close)
    
    def next(self, ts, sigma, spot):
        return self.helper.next(sigma)


class TsOpenSigmaCloseHelper:
    """
    组合一下 Ts Open 的快速开仓和 Sigma Close 在躲避大反弹的时候平仓。
    """
    def __init__(self, ts_open, ts_close, sigma_close):
        self.ts_open = ts_open
        self.ts_close = ts_close
        self.sigma_close = sigma_close
        self.ts_state = 0
        self.state = 0
    
    def next(self, ts, sigma, spot):
        if self.state == 0:
            if self.ts_state == 0 and ts > self.ts_open:
                self.ts_state = 1
                self.state = 1
            elif self.ts_state == 0 and ts < -1 * self.ts_open:
                self.ts_state = -1
                self.state = -1
            elif self.ts_state == 1 and ts < self.ts_close:
                # print('ts reset from 1')
                self.ts_state = 0
            elif self.ts_state == -1 and ts > -1 * self.ts_close:
                # print('ts reset from -1')
                self.ts_state = 0

        elif self.state == 1:
            if ts < self.ts_close:
                self.ts_state = 0
                self.state = 0
            elif sigma < self.sigma_close:
                self.state = 0
        elif self.state == -1:
            if ts > -1 * self.ts_close:
                self.ts_state = 0
                self.state = 0
            elif sigma > -1 * self.sigma_close:
                self.state = 0
        
        return self.state

class TsOpenSigmaReopenHelper:
    """ 这个的灵感是来自于发现 Sigma Close 之后根据 Sigma 的反弹还会有不错的盈利机会得到。"""
    def __init__(self, ts_open, ts_close, sigma_open, sigma_close):
        self.ts_open = ts_open
        self.ts_close = ts_close
        self.sigma_open = sigma_open
        self.sigma_close = sigma_close
        self.state = 0
    
    def next(self, ts, sigma, spot):
        if self.state == 0:
            if ts > self.ts_open and sigma > self.sigma_open:
                self.state = 1
            elif ts < -1 * self.ts_open and sigma < -1 * self.sigma_open:
                self.state = -1
        elif self.state == 1:
            if ts < self.ts_close:
                self.state = 0
            elif sigma < self.sigma_close:
                self.state = 0
        elif self.state == -1:
            if ts > -1 * self.ts_close:
                self.state = 0
            elif sigma > -1 * self.sigma_close:
                self.state = 0
        return self.state


class TsOpenTakeProfitHelper:
    """
    Ts Open 的快速开仓和 Take Profit 在高点回落的时候及时止盈平仓。
    """
    def __init__(self, ts_open, ts_close, stop_loss):
        self.ts_open = ts_open
        self.ts_close = ts_close
        self.stop_loss = stop_loss
        self.ts_state = 0
        self.state = 0
        self.spot_min = 100000
        self.spot_max = -100000
    
    def next(self, ts, sigma, spot_price):
        # 这个 if 让它开仓之后在选择记录最高点和最低点（这一点带来的影响不太清楚）。
        # 就是说如果没有这个 if ，当前面已经有非常高的高峰，那么这个开仓信号就会被忽略。
        if self.state == 0:
            self.spot_min = 100000
            self.spot_max = -100000
        else:
            self.spot_min = min(self.spot_min, spot_price)
            self.spot_max = max(self.spot_max, spot_price)
        if self.state == 0:
            if self.ts_state == 0 and ts > self.ts_open:
                self.ts_state = 1
                self.state = 1
            elif self.ts_state == 0 and ts < -1 * self.ts_open:
                self.ts_state = -1
                self.state = -1
        elif self.state == 1:
            if ts < self.ts_close:
                self.ts_state = 0
                self.state = 0
            elif spot_price / self.spot_max < 1 - self.stop_loss:
                self.state = 0
        elif self.state == -1:
            if ts > -1 * self.ts_close:
                self.ts_state = 0
                self.state = 0
            elif spot_price / self.spot_min > 1 + self.stop_loss:
                self.state = 0
        return self.state

# 单纯的 stop loss 给出了和 ts 差不多的成绩，但是亏损可控了。
# 这里可以再深入加强一下，改成抗单 1% 但是如果盈利超过 1% 那么在回落 0.3% 的时候止盈
# 可能这个回落 0.3 也有点太少了。
# 是否可以在盈利超过一定程度的时候接受来自 sigma 的平仓信号，否则选择回落 1% 的止盈。
# Sigma 平仓信号目测的损失还不如回落止盈。

# 更加科学的统计方法是绘制一个，每一笔开仓过程里最大盈利和最大亏损和平仓受益的相关性点阵图。
# 这个应该用 signal + count not zero 的方法，然后标记出每一个仓位状态下的编号，然后再用 group by 聚合。


def calc_long_short_pos(df: pd.DataFrame, wide: bool):
    """
    计算 long short pos
    """
    # ts signal 是一个均线策略
    ts_helper = TsOpenHelper(ts_open=400, ts_close=100)
    # sigma signal 是一个震荡器策略
    sigma_helper = SigmaOpenHelper(sigma_open=220, sigma_close=10)
    # ts open sigma close
    toss_helper = TsOpenSigmaCloseHelper(
            ts_open=400,
            ts_close=100, 
            sigma_close=-150)
    # ts open sigma reopen
    tosr_helper = TsOpenSigmaReopenHelper(
            ts_open=400,
            ts_close=100,
            sigma_open=-50,
            sigma_close=-150)
    # ts open take profit
    totp_helper = TsOpenTakeProfitHelper(
            ts_open=400,
            ts_close=100,
            stop_loss=0.01)

    pos_dict = {
        'ts_pos': { 'pos': [], 'helper': ts_helper },
        'sigma_pos': { 'pos': [], 'helper': sigma_helper },
        'toss_pos': { 'pos': [], 'helper': toss_helper },
        'tosr_pos': { 'pos': [], 'helper': tosr_helper },
        'totp_pos': { 'pos': [], 'helper': totp_helper },
    }
    for idx, row in df.iterrows():
        if (row['dt'].hour == 9
                or row['dt'].hour == 10 and row['dt'].minute < 10
                or row['dt'].hour == 14 and row['dt'].minute > 47
                or row['dt'].hour == 15):
            for key in pos_dict:
                pos_dict[key]['pos'].append(0)
            continue
        cols = getTradeCols(wide=wide)
        input = [
            row[cols['ts_col']],
            row[cols['sigma_col']],
            row[cols['spot_col']]
        ]
        for key in pos_dict:
            pos_dict[key]['pos'].append(pos_dict[key]['helper'].next(*input))
    for key in pos_dict:
        df[key] = pos_dict[key]['pos']
    return df

def pos2signal(df: pd.DataFrame, from_col: str, to_col: str):
    """
    将 pos 转换为 signal
    """
    from_se: pd.Series = df[from_col]
    diffhelper = DiffHelper()
    for idx, value in from_se.items():
        df.at[idx, to_col] = diffhelper.next(value)
    return df

def calc_buy_sell_signal(df: pd.DataFrame):
    df['ts_sigma_pos'] = np.where(df['ts_pos'] == df['sigma_pos'], df['ts_pos'], 0)
    pos_cols = [x for x in df.columns if '_pos' in x]
    for col in pos_cols:
        df = pos2signal(df, col, col.replace('_pos', '_signal'))
    return df

def calc_csv(df: pd.DataFrame, wide: bool):
    df['dt'] = pd.to_datetime(df['dt'])
    df = calc_long_short_pos(df, wide=wide)
    df = calc_buy_sell_signal(df)
    signal_cols = [x for x in df.columns if '_signal' in x]
    df = df[['dt', 'spot_price', *signal_cols]]
    # keep rows where signal cols is not zero only.
    print(df.loc[(df[signal_cols] != 0).any(axis=1)])
    return df

def calc_signal_csv(spot: str, suffix: str, wide: bool):
    suffix += gen_wide_suffix(wide)
    df = pd.read_csv(DATA_DIR / 'dsp_conv' / f'stats_{spot}_{suffix}.csv')
    df = calc_csv(df, wide=wide)
    df.to_csv(DATA_DIR / 'dsp_conv' / f'signal_{spot}_{suffix}.csv', index=False)

@click.command()
@click.option('-s', '--spot', type=str, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, help="csv file name suffix.")
@click.option('--wide', type=bool, default=False, help="use wide data.")
def click_main(spot: str, suffix: str, wide: bool):
    calc_signal_csv(spot, suffix, wide=wide)

if __name__ == '__main__':
    click_main()

"""
Ts 的参数现在还是一个重点调整对象。
除非是那种长牛趋势的日子，看起来现在的平仓信号只会太晚不会太早。
所以需要加入其他的平仓规则，例如说盘整就止盈，例如 PCP 止盈。
两个指标分开计算盈利然后加权也是一种做法。
或者平仓的时候两个指标分开平仓。

ts trades 在大反弹的日子里面很容易亏爆。
ts sigma trades 容易抓不住长期趋势。
现在来看还是 ts trades 的总体盈利多一点。
但是 ts trades 的平仓速度太慢。
要是加上 pcp 在强烈信号的时候平仓应该会更加顺利一些。

！试试开仓用 ts 信号，平仓用 dirstd 或者 pcp 信号。

12 月使用 1 月的规则会亏爆 我当时用的是 wide 参数。
12 月 10 日是 wide 参数最突显作用的一天。
12 月 10 日是 ts trades 亏损最大的一天。
"""

