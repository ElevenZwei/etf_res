"""
这个脚本需要拓展 s7 里面的交易信号。
尝试各种不同的交易信号，保存到 csv  文件里。
"""

import click
import datetime
import pandas as pd
import numpy as np

from dsp_config import DATA_DIR, gen_wide_suffix
from helpers import OpenCloseHelper, DiffHelper, TsOpenHelper, SigmaOpenHelper
from helpers import TsOpenSigmaCloseHelper, TsOpenSigmaReopenHelper, TsOpenTakeProfitHelper
from st_runner import StrategyArgs, StrategyRunner

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


def calc_long_short_pos(df: pd.DataFrame, wide: bool):
    """
    计算 long short pos
    """
    # ts signal 是一个均线策略
    ts_helper = TsOpenHelper()
    ts_helper.config({
        'ts_open': 400,
        'ts_close': 100,
    })
    # sigma signal 是一个震荡器策略
    sigma_helper = SigmaOpenHelper()
    sigma_helper.config({
        'sigma_open': 220,
        'sigma_close': 10,
    })
    # ts open sigma close
    toss_helper = TsOpenSigmaCloseHelper()
    toss_helper.config({
        'ts_open': 400,
        'ts_close': 100,
        'sigma_close': -150,
        'p2p_stop_loss': 0.03,
    })
    toss2_helper = TsOpenSigmaCloseHelper()
    toss2_helper.config({
        'ts_open': 350,
        'ts_close': 100,
        'sigma_close': -20,
        'p2p_stop_loss': 0.03,
    })
    toss3_helper = TsOpenSigmaCloseHelper()
    toss3_helper.config({
        'ts_open': 300,
        'ts_close': 100,
        'sigma_close': -20,
        'p2p_stop_loss': 0.03,
    })
    toss4_helper = TsOpenSigmaCloseHelper()
    toss4_helper.config({
        'ts_open': 300,
        'ts_close': 100,
        'sigma_close': -20,
        'p2p_stop_loss': 0.01,
    })
    # ts open sigma reopen
    tosr_helper = TsOpenSigmaReopenHelper()
    tosr_helper.config({
        'ts_open': 300,
        'ts_close': 100,
        'sigma_open': 150,
        'sigma_close': 20,
    })
    # ts open take profit
    totp_helper = TsOpenTakeProfitHelper()
    totp_helper.config({
        'ts_open': 400,
        'ts_close': 100,
        'stop_loss': 0.01,
    })

    pos_dict = {
        'ts_pos': { 'pos': [], 'helper': ts_helper },
        'sigma_pos': { 'pos': [], 'helper': sigma_helper },
        'toss_pos': { 'pos': [], 'helper': toss_helper },
        'toss2_pos': { 'pos': [], 'helper': toss2_helper },
        'toss3_pos': { 'pos': [], 'helper': toss3_helper },
        'toss4_pos': { 'pos': [], 'helper': toss4_helper },
        'tosr_pos': { 'pos': [], 'helper': tosr_helper },
        'totp_pos': { 'pos': [], 'helper': totp_helper },
    }
    for idx, row in df.iterrows():
        if (row['dt'].hour == 9 and row['dt'].minute < 55
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
        if input[2] == 0:
            print(f'spot is zero at {row}')
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

def calc_signals(df: pd.DataFrame, wide: bool):
    runner = StrategyRunner()
    st_args = StrategyArgs(
            time_begin=datetime.time(9, 55),
            time_end=datetime.time(14, 47),
            dirstd_ts_len=600,
            dirstd_sigma_width=(0.4 if wide else 0.15))
    runner.addStrategy('ts1', 'ts', st_args.clone().config({
        'ts_open': 400,
        'ts_close': 100,
    }))
    runner.addStrategy('sigma1', 'sigma', st_args.clone().config({
        'sigma_open': 220,
        'sigma_close': 10,
    }))
    runner.addStrategy('toss1', 'toss', st_args.clone().config({
        'ts_open': 400,
        'ts_close': 100,
        'sigma_close': -150,
        'p2p_stop_loss': 0.03,
    }))
    runner.addStrategy('toss2', 'toss', st_args.clone().config({
        'ts_open': 350,
        'ts_close': 100,
        'sigma_close': -20,
        'p2p_stop_loss': 0.03,
    }))
    runner.addStrategy('toss3', 'toss', st_args.clone().config({
        'ts_open': 300,
        'ts_close': 100,
        'sigma_close': -20,
        'p2p_stop_loss': 0.03,
    }))
    runner.addStrategy('toss4', 'toss', st_args.clone().config({
        'ts_open': 300,
        'ts_close': 100,
        'sigma_close': -20,
        'p2p_stop_loss': 0.01,
    }))
    runner.addStrategy('tosr1', 'tosr', st_args.clone().config({
        'ts_open': 300,
        'ts_close': 100,
        'sigma_open': 150,
        'sigma_close': 20,
    }))
    runner.addStrategy('totp1', 'totp', st_args.clone().config({
        'ts_open': 400,
        'ts_close': 100,
        'stop_loss': 0.01,
    }))
    runner.addStrategy('totp2', 'totp', st_args.clone().config({
        'ts_open': 350,
        'ts_close': 100,
        'stop_loss': 0.01,
    }))

    runner.addData(df)
    sig = pd.DataFrame(runner.readSignal())
    df = pd.concat([df, sig], axis=1)
    return df

def calc_csv(df: pd.DataFrame, wide: bool):
    df['dt'] = pd.to_datetime(df['dt'])
    # df = calc_long_short_pos(df, wide=wide)
    # df = calc_buy_sell_signal(df)
    df = calc_signals(df, wide=wide)
    signal_cols = [x for x in df.columns if '_signal' in x]
    df = df[['dt', 'spot_price', *signal_cols]]
    # print rows where signal cols is not zero only.
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

2024-09-30 这一天没有不同 Sigma 之间的差异，需要用非常 wide 的参数才有用。
好像今年的这些参数在 2024 年 9 月除了月底的趋势，几乎不赚钱，看起来日内做方向可以赚钱是一种错觉吗？
我感到非常挫败。样本大一点做什么都没用，相关性可以完全被挫败，所有的个案修正只能用于个案。

====
我发现了一些事情。
高回落止盈和纯抗单止损可以分开做成两个阈值。
toss3 可以用 0.5% 的纯抗单止损，因为再往下的收益都是负数。
totp 可以分成两个参数，一个是高回落参数，一个是纯抗单参数。
现在的情况是怎样的高回落可以有再次出发的可能性这一点很难预计。
我应该用 max drawdown 算法计算出每一次 ts 平仓之前的 Drawdown ，这个放在新的。

"""

