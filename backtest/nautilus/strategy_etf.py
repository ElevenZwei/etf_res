import math
import pandas as pd
import datetime
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.instruments import Instrument, Equity
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import OrderSide

from backtest.nautilus.data_types import MyQuoteTick

class StrategyETFConfig(StrategyConfig, frozen=True):
    spot: Instrument = None
    venue: Venue = None
    size_mode: int = None

class StrategyETF(Strategy):
    def __init__(self, config: StrategyETFConfig):
        super().__init__(config)
        self.spot = config.spot
        self.size_mode = config.size_mode
        self.hold_id = None
        self.hold_from: datetime.datetime = None
        self.hold_action = None
        
    def get_cash(self):    
        cash = self.portfolio.account(self.config.venue).balance().total.as_double()
        return cash
    
    def get_net_worth(self):
        cash = self.portfolio.account(self.config.venue).balance().total.as_double()
        pos_value = 0
        if self.hold_id is not None:
            pos_value = self.portfolio.net_exposure(self.hold_id).as_double()
        sum = cash + pos_value
        # self.log.info(f"account: cash={cash}, pos={pos_value}, sum={sum}")
        return sum
    
    def on_start(self):
        self.log.info('on_start: subscribe all contracts.')
        self.subscribe_quote_ticks(self.spot.id)
    
    def on_quote_tick(self, tick: MyQuoteTick):
        self.on_spot_tick(tick)
    
    def on_spot_tick(self, tick: MyQuoteTick):
        # self.log.info(f'net_worth={self.get_net_worth()}')

        now = self.clock.utc_now() 
        self.log.info(f"now={now}")
        if (now.hour == 6 and now.minute > 30) or now.hour > 6:
            self.log.info(f"now is after 14:30, close all.")
            self.close_all()
            return
        if (now.hour == 1 and now.minute < 40):
            self.log.info(f"now is before 9:40, skip this.")
            return

        spot_price = tick.ask_price
        spot_action = tick.action
        if spot_action is None:
            self.log.info(f"spot action is none.")
            return
        if self.size_mode == 5 or self.size_mode == 6:
            # long only mode
            if spot_action < 0:
                self.log.info(f"spot action is negative, skip this.")
                spot_action = 0
        if self.size_mode == 7 or self.size_mode == 8:
            # short only mode
            if spot_action > 0:
                self.log.info(f"spot action is positive, skip this.")
                spot_action = 0

        if self.hold_action == spot_action:
            self.log.info(f"hold action is same, skip this.")
            return

        self.log.info(f'spot price={spot_price}, action={spot_action}')
        self.close_all()
        if spot_action == 0:
            self.log.info(f"spot action is 0, skip this.")
            return

        # Make order 
        inst = self.spot
        inst_id = tick.instrument_id
        last_quote = self.cache.quote_tick(inst_id)
        if last_quote is None:
            self.log.info("cannot read opt md, skip this.")
            return
        if last_quote.ask_price == 0:
            self.log.info(f"last ask_price is zero, skip this, price={repr(last_quote)}")
            return

        askp = last_quote.ask_price.as_double()
        trade_size = self.get_cash() / askp // 10000 * 10000

        trade_size *= abs(spot_action)
        if trade_size < 10000:
            self.log.info(f"trade size is too small, skip this, size={trade_size}")
            return
        self.hold_id = inst_id
        self.hold_from = now
        self.hold_action = spot_action
        order = self.order_factory.market(
            instrument_id=inst_id,
            order_side=OrderSide.BUY if spot_action > 0 else OrderSide.SELL,
            quantity=inst.make_qty(trade_size),
            time_in_force=TimeInForce.FOK,
        )
        self.submit_order(order)

    def close_all(self):
        if self.hold_id is None:
            return
        self.close_all_positions(self.hold_id)
        self.hold_id = None
        self.hold_action = 0

