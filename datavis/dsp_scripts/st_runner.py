import numpy as np
import pandas as pd
import json

import helpers

class StrategyArgs():
    def __init__(self, time_begin, time_end, dirstd_ts_len, dirstd_sigma_width):
        self.time_begin = time_begin
        self.time_end = time_end
        self.ts_len = dirstd_ts_len
        self.sigma_width = dirstd_sigma_width
        self.st_args = None
    
    def config(self, st_args):
        self.st_args = st_args
        return self
    
    def clone(self):
        st = StrategyArgs(  
                self.time_begin, self.time_end,
                self.ts_len, self.sigma_width)
        st.config(self.st_args)
        return st
    
    def json(self):
        obj = {
            'time': {
                'begin': self.time_begin,
                'end': self.time_end,
            },
            'col': {
                'ts': self.ts_len,
                'sigma': self.sigma_width,
            },
            'st': self.st_args
        }
        json_str = json.dump(obj)
        return json_str


class StrategyRecord():
    def __init__(self, helper_class, args: StrategyArgs):
        self.helper = helper_class()
        self.helper.config(args.st_args)
        self.diff = helpers.DiffHelper()
        self.pos = []
        self.act = []
        self.args = args
        self.ts_col = f'oi_cp_dirstd_ts_{args.ts_len}'
        self.sigma_col = f'oi_cp_dirstd_sigma_{args.sigma_width}'
    
    def next(self, row):
        dt = row['dt'].time()
        if dt < self.args.time_begin or dt > self.args.time_end:
            self.pos.append(0)
            self.act.append(self.diff.next(0))
            return

        ts = row[self.ts_col]
        sigma = row[self.sigma_col]
        spot = row['spot_price']
        next_pos = self.helper.next(ts, sigma, spot)
        self.pos.append(next_pos)
        self.act.append(self.diff.next(next_pos))
        

class StrategyRunner():
    """
    这个类需要满足哪些需求？
    1. 加入需要运行的小策略
    2. 小策略有自己的参数和配置
    3. 加入需要运行的数据，需要流式注入的功能
    4. 根据配置的运行时间和配置要读取的数据列，给小策略对应的数据
    5. 把小策略输出的仓位和动作储存下来，提供一个 read 函数导出
    6. 提供一个上传函数把策略和参数信息和运行结果上传到数据库里。
    """
    def __init__(self):
        self.strategy_map = {
            'ts': helpers.TsOpenHelper,
            'sigma': helpers.SigmaOpenHelper,
            'toss': helpers.TsOpenSigmaCloseHelper,
            'totp': helpers.TsOpenTakeProfitHelper,
            'tosr': helpers.TsOpenSigmaReopenHelper,
        }
        self.run = {}

    def addStrategy(self, name, st_name, args: StrategyArgs):
        self.run[name] = StrategyRecord(
                self.strategy_map[st_name], args)
    
    def addData(self, df: pd.DataFrame):
        for idx, row in df.iterrows():
            for name in self.run:
                self.runDataRow(name, row)

    def runDataRow(self, name, row):
        frame = self.run[name]
        frame.next(row)
    
    def readSignal(self):
        return {f'{name}_signal':
                self.run[name].act for name in self.run}
    
