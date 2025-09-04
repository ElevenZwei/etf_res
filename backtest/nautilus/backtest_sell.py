"""
这个脚本从外界文件中读取市场数据和交易信号，
构建期权卖出的仓位，输出回测的结果。
期权卖出的策略是 strategy_sell.py
这是一个卖出指定 Delta 值 Call 或者 Put 期权的策略。
"""

import datetime
import click
import pandas as pd

from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig

from backtest.config import DATA_DIR
from backtest.nautilus.data_types import prepare_venue, prepare_spot_quote_from_df, prepare_option_quote
from backtest.nautilus.strategy_sell import StrategySell, StrategySellConfig

def run(size_mode: int, suffix: str, column: str = 'position'):
    # bgdt = datetime.date(2024, 1, 1)
    # eddt = datetime.date(2024, 10, 1)
    bgdt = datetime.date(2025, 1, 1)
    # eddt = datetime.date(2025, 4, 1)
    eddt = datetime.date(2025, 8, 19)

    engine = BacktestEngine(config=BacktestEngineConfig(
        trader_id=TraderId('BT-001'),
    ))
    venue_name = 'sim'
    ven = prepare_venue(engine, venue_name)

    # spot_df = pd.read_csv(f'{DATA_DIR}/input/oi_spot_159915.csv')
    spot_df = pd.read_csv(f'{DATA_DIR}/input/spot_159915_2025_dsp.csv')
    if spot_df['code'].dtype != str:
        spot_df['code'] = spot_df['code'].astype('Int64').astype(str)
    spot_df['dt'] = pd.to_datetime(spot_df['dt'])
    spot_df = spot_df.set_index('dt')

    # action_df = pd.read_csv(f'{DATA_DIR}/input/zxt_stock_position.csv')
    # action_df = pd.read_csv(f'{DATA_DIR}/cpr/roll_merged_1.csv')
    action_df = pd.read_csv(f'{DATA_DIR}/cc/cc_position_159949_2025.csv')
    action_df['dt'] = pd.to_datetime(action_df['dt'])
    action_df = action_df.set_index('dt')
    
    # action_df = pd.read_parquet(f'{DATA_DIR}/input/zxt_mask_position.parquet', engine='pyarrow')
    action_df['action'] = action_df[column]

    spot_inst = prepare_spot_quote_from_df(
        spot_df, action_df, engine, ven, bgdt, eddt)
    opt_info = prepare_option_quote(
        # f'{DATA_DIR}/input/tl_greeks_159915_all_fixed.csv',
        f'{DATA_DIR}/input/opt_159915_2025_greeks.csv',
        engine, ven, bgdt, eddt)

    suffix=f"sell_m{size_mode}_{suffix}"
    sell_config = StrategySellConfig(
        spot=spot_inst, infos=opt_info, venue=ven,
        size_mode=size_mode, sell_delta=0.6,)
    sell_st = StrategySell(config=sell_config)
    engine.add_strategy(strategy=sell_st)
    result = engine.run()

    engine.trader.generate_account_report(ven).to_csv(f'{DATA_DIR}/output/opt_account_{suffix}.csv')
    engine.trader.generate_order_fills_report().to_csv(f'{DATA_DIR}/output/opt_order_{suffix}.csv')
    engine.trader.generate_positions_report().to_csv(f'{DATA_DIR}/output/opt_pos_{suffix}.csv')
    engine.reset()
    engine.dispose()

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-m', '--size-mode', type=int, help='size mode: 1 2 3 4')
@click.option('-s', '--suffix', type=str, default='',)
def click_main(size_mode: int, suffix: str):
    run(size_mode, suffix,
        column='position',
        # column=f'{suffix}_position' if suffix else 'position'
    )

if __name__ == '__main__':
    click_main()


