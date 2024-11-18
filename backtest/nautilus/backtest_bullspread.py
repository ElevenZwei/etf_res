import datetime
import click

from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig

from data_types import prepare_venue, prepare_spot_quote, prepare_option_quote
from strategy_bullspread import StrategyBullSpread, StrategyBullSpreadConfig

def run(mode: int):
    bgdt = datetime.date(2024, 2, 1)
    eddt = datetime.date(2024, 12, 1)

    engine = BacktestEngine(config=BacktestEngineConfig(
        trader_id=TraderId('BT-001'),
    ))
    venue_name = 'sim'
    ven = prepare_venue(engine, venue_name)
    if mode == 1:
        spot_inst = prepare_spot_quote(
            '../input/oi_signal_159915_act_changes.csv',
            engine, ven, bgdt, eddt)
    elif mode == 2:
        spot_inst = prepare_spot_quote(
            '../input/oi_signal_159915_act_full.csv',
            engine, ven, bgdt, eddt)
    opt_info = prepare_option_quote(
        '../input/tl_greeks_159915_all_fixed.csv',
        engine, ven, bgdt, eddt)

    suffix=f"{mode}"
    config = StrategyBullSpreadConfig(
        mode=mode,
        spot=spot_inst,
        infos=opt_info,
        venue=ven,
        hold_days_limit=100 if mode == 1 else 15,
        long_buy_delta=-0.3,
        long_sell_delta=-0.5,
        short_buy_delta=0.3,
        short_sell_delta=0.5,
    )
    bull_str = StrategyBullSpread(config=config)
    engine.add_strategy(strategy=bull_str)
    result = engine.run()

    engine.trader.generate_account_report(ven).to_csv(f'../output/opt_bullsp_account_{suffix}.csv')
    engine.trader.generate_order_fills_report().to_csv(f'../output/opt_bullsp_order_{suffix}.csv')
    engine.trader.generate_positions_report().to_csv(f'../output/opt_bullsp_pos_{suffix}.csv')
    engine.reset()
    engine.dispose()

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-m', '--mode', type=int, help='size mode: 1 2 3 4')
def click_main(mode: int):
    run(mode)

if __name__ == '__main__':
    click_main()