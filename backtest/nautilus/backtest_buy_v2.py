"""
这个脚本从外界文件中读取市场数据和交易信号，
构建期权买入的仓位，输出回测的结果。
期权买入的策略是 strategy_buy.py
这是一个买入平值 Call 或者 Put 期权的策略。
每一次换向都会平掉之前的仓位。
"""

import datetime
import click
import pandas as pd

from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig

from backtest.config import DATA_DIR
from backtest.nautilus.data_types import prepare_venue, prepare_spot_quote_from_df, prepare_option_quote
from backtest.nautilus.strategy_buy import StrategyBuy, StrategyBuyConfig

def run(size_mode: int):
    bgdt = datetime.date(2024, 2, 1)
    eddt = datetime.date(2024, 3, 1)

    engine = BacktestEngine(config=BacktestEngineConfig(
        trader_id=TraderId('BT-001'),
    ))
    venue_name = 'sim'
    ven = prepare_venue(engine, venue_name)

    spot_df = pd.read_csv(f'{DATA_DIR}/input/oi_spot_159915.csv')
    spot_df['dt'] = pd.to_datetime(spot_df['dt'])
    spot_df = spot_df.set_index('dt')

    action_df = pd.read_csv(f'{DATA_DIR}/input/zxt_mask_action_changes.csv')
    action_df['dt'] = pd.to_datetime(action_df['dt'])
    action_df['action'] = action_df['mask_action']
    action_df = action_df.set_index('dt')

    spot_inst = prepare_spot_quote_from_df(
        spot_df, action_df, engine, ven, bgdt, eddt)
    opt_info = prepare_option_quote(
        f'{DATA_DIR}/input/tl_greeks_159915_clip_fixed.csv',
        engine, ven, bgdt, eddt)

    suffix=f"{size_mode}"
    buy_config = StrategyBuyConfig(
        spot=spot_inst, infos=opt_info, venue=ven,
        hold_days_limit=3,
        size_mode=size_mode,
        impv_min=0.2, impv_max=0.4)
    buy_st = StrategyBuy(config=buy_config)
    engine.add_strategy(strategy=buy_st)
    result = engine.run()

    engine.trader.generate_account_report(ven).to_csv(f'../output/opt_account_{suffix}.csv')
    engine.trader.generate_order_fills_report().to_csv(f'../output/opt_order_{suffix}.csv')
    engine.trader.generate_positions_report().to_csv(f'../output/opt_pos_{suffix}.csv')
    engine.reset()
    engine.dispose()

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-s', '--size-mode', type=int, help='size mode: 1 2 3 4')
def click_main(size_mode: int):
    run(size_mode)

if __name__ == '__main__':
    click_main()


