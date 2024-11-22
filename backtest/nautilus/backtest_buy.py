# 这个尝试在 ETF 的平值附近根据信号操作
import datetime
import click

from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig

from backtest.nautilus.data_types import prepare_venue, prepare_spot_quote, prepare_option_quote
from backtest.nautilus.strategy_buy import StrategyBuy, StrategyBuyConfig

def run(size_mode: int):
    bgdt = datetime.date(2024, 2, 1)
    eddt = datetime.date(2024, 12, 1)

    engine = BacktestEngine(config=BacktestEngineConfig(
        trader_id=TraderId('BT-001'),
    ))
    venue_name = 'sim'
    ven = prepare_venue(engine, venue_name)
    spot_inst = prepare_spot_quote(
        '../input/oi_signal_159915_act_changes.csv',
        engine, ven, bgdt, eddt)
    opt_info = prepare_option_quote(
        '../input/tl_greeks_159915_clip_fixed.csv',
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

