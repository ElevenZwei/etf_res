import datetime
import click

from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig

from backtest.config import DATA_DIR
from backtest.nautilus.data_types import prepare_venue, prepare_spot_quote, prepare_option_quote
from backtest.nautilus.strategy_bullspread_v2 import StrategyBullSpread2, StrategyBullSpread2Config

def run(mode: int):
    # bgdt = datetime.date(2024, 3, 1)
    # eddt = datetime.date(2024, 4, 1)
    bgdt = datetime.date(2024, 9, 1)
    eddt = datetime.date(2024, 11, 1)

    engine = BacktestEngine(config=BacktestEngineConfig(
        trader_id=TraderId('BT-001'),
    ))
    venue_name = 'sim'
    ven = prepare_venue(engine, venue_name)
    spot_inst = prepare_spot_quote(
        f'{DATA_DIR}/input/oi_spot_159915.csv',
        # f'{DATA_DIR}/input/nifty_oi.csv',
        engine, ven, bgdt, eddt)
    opt_info = prepare_option_quote(
        f'{DATA_DIR}/input/tl_greeks_159915_all_fixed.csv',
        # f'{DATA_DIR}/input/nifty_greeks_combined.csv',
        engine, ven, bgdt, eddt)

    suffix=f"{mode}"
    config = StrategyBullSpread2Config(
        mode=mode,
        spot=spot_inst,
        infos=opt_info,
        venue=ven,
        long_buy_delta=-0.3 if mode == 1 else -0.1,
        long_sell_delta=-0.5,
        short_buy_delta=0.3 if mode == 1 else 0.1,
        short_sell_delta=0.5,
        # open_amount=5_0000,
        open_amount=100,
        base_oi_interval=22 * 60,
        diff_oi_threshold=4000,
        # cash_usage=10_0000,
        cash_usage=100_0000,
    )
    bull_str = StrategyBullSpread2(config=config)
    engine.add_strategy(strategy=bull_str)
    engine.run()

    engine.trader.generate_account_report(ven).to_csv(f'{DATA_DIR}/output/opt_bullsp_account_{suffix}.csv')
    engine.trader.generate_order_fills_report().to_csv(f'{DATA_DIR}/output/opt_bullsp_order_{suffix}.csv')
    engine.trader.generate_positions_report().to_csv(f'{DATA_DIR}/output/opt_bullsp_pos_{suffix}.csv')
    engine.reset()
    engine.dispose()

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-m', '--mode', type=int, help='size mode: 1 2 3 4')
def click_main(mode: int):
    run(mode)

if __name__ == '__main__':
    click_main()