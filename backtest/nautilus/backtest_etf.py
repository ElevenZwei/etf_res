"""
这个脚本从外界文件中读取市场数据和交易信号，
构建 ETF 交易的仓位。
"""

import datetime
import click
import pandas as pd

from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig

from backtest.config import DATA_DIR
from backtest.nautilus.data_types import prepare_venue, prepare_spot_quote_from_df, prepare_option_quote
from backtest.nautilus.strategy_etf import StrategyETF, StrategyETFConfig

def run(size_mode: int, suffix: str, column: str = 'pcr_position'):
    # bgdt = datetime.date(2024, 2, 1)
    # eddt = datetime.date(2024, 10, 1)
    bgdt = datetime.date(2025, 1, 1)
    eddt = datetime.date(2025, 5, 27)

    engine = BacktestEngine(config=BacktestEngineConfig(
        trader_id=TraderId('BT-001'),
    ))
    venue_name = 'sim'
    ven = prepare_venue(engine, venue_name)

    # spot_df = pd.read_csv(f'{DATA_DIR}/input/oi_spot_159915.csv')
    spot_df = pd.read_csv(f'{DATA_DIR}/input/spot_159915_2025_dsp.csv')
    spot_df['dt'] = pd.to_datetime(spot_df['dt'])
    if spot_df['code'].dtype != str:
        spot_df['code'] = spot_df['code'].astype('Int64').astype(str)
    spot_df = spot_df.set_index('dt')

    action_df = pd.read_csv(f'{DATA_DIR}/input/zxt_stock_position.csv')
    action_df['dt'] = pd.to_datetime(action_df['dt'])
    action_df = action_df.set_index('dt')
    # action_df = pd.read_parquet(f'{DATA_DIR}/input/zxt_mask_position.parquet', engine='pyarrow')
    action_df['action'] = action_df[column]
    spot_inst = prepare_spot_quote_from_df(
        spot_df, action_df, engine, ven, bgdt, eddt)
    
    suffix=f"etf_m{size_mode}_{suffix}"
    etf_config = StrategyETFConfig(
        spot=spot_inst, venue=ven, size_mode=size_mode)
    etf_st = StrategyETF(config=etf_config)
    engine.add_strategy(strategy=etf_st)
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
    run(size_mode, suffix, column=f'{suffix}_position')

if __name__ == '__main__':
    click_main()



