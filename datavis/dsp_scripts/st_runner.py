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
                'begin': self.time_begin.strftime('%H:%M:%S'),
                'end': self.time_end.strftime('%H:%M:%S'),
            },
            'col': {
                'ts': self.ts_len,
                'sigma': self.sigma_width,
            },
            'st': self.st_args
        }
        json_str = json.dumps(obj)
        return json_str


class StrategyRecord():
    def __init__(self, st_name, helper_class, args: StrategyArgs):
        self.st_name = st_name
        self.helper = helper_class()
        self.helper.config(args.st_args)
        self.diff = helpers.DiffHelper()
        self.pos = []
        self.act = []
        self.dt = []
        self.args = args
        self.ts_col = f'oi_cp_dirstd_ts_{args.ts_len}'
        self.sigma_col = f'oi_cp_dirstd_sigma_{args.sigma_width}'
        self.last_input = None
    
    def next(self, row):
        self.dt.append(row['dt'])
        now = row['dt'].time()
        if now < self.args.time_begin or now > self.args.time_end:
            self.pos.append(0)
            self.act.append(self.diff.next(0))
            return

        ts = row[self.ts_col]
        sigma = row[self.sigma_col]
        spot = row['spot_price']
        self.last_input=(ts, sigma, spot)
        next_pos = self.helper.next(*self.last_input)
        self.pos.append(next_pos)
        self.act.append(self.diff.next(next_pos))

# db part
import sqlalchemy
from s0_md_query import get_engine

class StrategyUploader:
    metadata = None
    engine = None

    @classmethod
    def initSql(cls):
        cls.metadata = sqlalchemy.MetaData()
        cls.engine = get_engine()

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
            'ts_sigma': helpers.TsSigmaOpenHelper,
            'toss': helpers.TsOpenSigmaCloseHelper,
            'totp': helpers.TsOpenTakeProfitHelper,
            'tosr': helpers.TsOpenSigmaReopenHelper,
        }
        self.run = {}

    def addStrategy(self, name, st_name, args: StrategyArgs):
        self.run[name] = StrategyRecord(
                st_name, self.strategy_map[st_name], args)
    
    def addData(self, df: pd.DataFrame):
        for idx, row in df.iterrows():
            for name in self.run:
                self.runDataRow(name, row)

    def runDataRow(self, name, row):
        frame = self.run[name]
        frame.next(row)

    def readLastInput(self):
        return {name: self.run[name].last_input for name in self.run}
    
    def readSignal(self):
        return {f'{name}_signal': self.run[name].act
                for name in self.run}
    
    def initSql(self):
        if StrategyUploader.engine is None:
            StrategyUploader.initSql()

    @staticmethod
    def upsert_on_conflict_skip(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = sqlalchemy.dialects.postgresql.insert(table.table).values(data)
        stmt = stmt.on_conflict_do_nothing()
        conn.execute(stmt)

    def uploadStrategy(self):
        # table = Table('trade_strategy',
        #       StrategyUploader.metadata, autoload_with=StrategyUploader.engine)
        df = pd.DataFrame({'st_name': self.strategy_map.keys()})
        df.to_sql('trade_strategy', StrategyUploader.engine,
                if_exists='append', index=False,
                method=self.upsert_on_conflict_skip)
    
    def uploadFrame(self):
        df = pd.DataFrame([{
                'arg_desc': name,
                'st_name': frame.st_name,
                'arg': frame.args.json(),
        } for name, frame in self.run.items()])
        df.to_sql('trade_strategy_args', StrategyUploader.engine,
                if_exists='append', index=False,
                method=self.upsert_on_conflict_skip)
    
    def uploadSignal(self):
        # 这个要先用
        for name, frame in self.run.items():
            df = pd.DataFrame([{
                    'arg_desc': name,
                    'dt': frame.dt[x],
                    'act': sig,
            } for x, sig in enumerate(frame.act)])
            df = df[df['act'] != 0]
            df.to_sql('trade_signal', StrategyUploader.engine,
                    if_exists='append', index=False,
                    method=self.upsert_on_conflict_skip)


"""
下面是关于一次运行之后的结果上传到数据库里面的思考。
我需要上传的东西有三张表，都按照 on conflict skip 的逻辑上传数据。
"""


