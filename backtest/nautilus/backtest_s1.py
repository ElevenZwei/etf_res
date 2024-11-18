# 这个文件尝试读取一个合约的指标
import math
import random
import pandas as pd
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.config import StrategyConfig
from nautilus_trader.common.enums import LogColor
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType, OmsType, TimeInForce
from nautilus_trader.model.objects import Money
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.persistence.loaders import CSVTickDataLoader, CSVBarDataLoader
from nautilus_trader.persistence.wranglers import QuoteTickDataWrangler
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.orders import MarketOrder


def clip_df():
    df = pd.read_csv('input/options_159915_minute_data.csv')
    symbol = df['code'][0]
    df = df[df['code'] == symbol]
    df.to_csv('input/clip.csv', index=False)
    
def prepare_venue(engine):
    fill_model = FillModel(
        prob_fill_on_limit=0.2,
        prob_fill_on_stop=0.95,
        prob_slippage=0,
        random_seed=42,
    )
    ven = Venue('SIM')
    engine.add_venue(
        venue=ven,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=USD,
        starting_balances=[Money(1_000_000, USD)],
        fill_model=fill_model,
    )
    return ven

def prepare_quotes(engine):
    df = CSVTickDataLoader.load('input/clip.csv', 'dt')
    symbols = df['code'].unique()
    insts = []
    for sym in symbols:
        df = df[df['code'] == sym]
        df['bid'] = df['closep']
        df['ask'] = df['closep']
        df['bid'] = -1
        contract_id = df['tradecode'][0]
        inst = TestInstrumentProvider.equity(contract_id, 'SIM')
        wrangler = QuoteTickDataWrangler(instrument=inst)
        engine.add_instrument(inst)
        insts.append(inst)
        engine.add_data(wrangler.process(df))
    return insts
        
class MyStConfig(StrategyConfig, frozen=True):
    inst: str = None
    
class MySt(Strategy):
    def __init__(self, config: MyStConfig):
        super().__init__(config)
        self.inst_id = config.inst.id
        self.inst = config.inst
        self.trade_size = 10
        self.ven = Venue('SIM')
    
    def on_start(self):
        print("on_start")
        self.subscribe_quote_ticks(self.inst_id)

    def on_quote_tick(self, tick: QuoteTick):
        self.log.info(repr(tick), LogColor.CYAN)
        self.log.info(repr(self.clock.utc_now()))
        self.log.info(repr(self.portfolio.account(self.ven).balances_total()))
        self.log.info(repr(self.portfolio.net_position(self.inst_id)))
        self.log.info(repr(self.portfolio.net_exposure(self.inst_id)))
        self.log.info(repr(tick.bid_price))
        rand =  random.randint(1, 10)
        if rand <= 2:
            order: MarketOrder = self.order_factory.market(
                instrument_id=self.inst_id,
                order_side=OrderSide.BUY,
                quantity=self.inst.make_qty(self.trade_size),
                time_in_force=TimeInForce.FOK,
            )
            # self.submit_order(order)
        elif rand >= 9:
            pass
            # self.close_all_positions(self.inst_id)
            # order: MarketOrder = self.order_factory.market(
            #     instrument_id=self.inst_id,
            #     order_side=OrderSide.SELL,
            #     quantity=self.inst.make_qty(self.trade_size),
            #     time_in_force=TimeInForce.FOK,
            # )
            # self.submit_order(order)
        

engine = BacktestEngine(config=BacktestEngineConfig(
    trader_id=TraderId('BT-001'),
))
ven = prepare_venue(engine)
symbols = prepare_quotes(engine)

config = MyStConfig(inst=symbols[0])
st = MySt(config=config)
engine.add_strategy(strategy=st)
result = engine.run()
# print(result)

# Optionally view reports
# with pd.option_context(
#     "display.max_rows",
#     100,
#     "display.max_columns",
#     None,
#     "display.width",
#     300,
# ):
    # print(engine.trader.generate_account_report(ven))
    # print(engine.trader.generate_order_fills_report())
    # print(engine.trader.generate_positions_report())

engine.trader.generate_account_report(ven).to_csv('output/account.csv')
engine.trader.generate_order_fills_report().to_csv('output/order.csv')
engine.trader.generate_positions_report().to_csv('output/pos.csv')
# For repeated backtest runs make sure to reset the engine
engine.reset()

# Good practice to dispose of the object when done
engine.dispose()


