import math
import pandas as pd
import datetime
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.instruments import Instrument, Equity
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import OrderSide

from backtest.nautilus.data_types import MyQuoteTick, OptionInfo

class StrategySellConfig(StrategyConfig, frozen=True):
    spot: Instrument = None
    infos: dict[Instrument, OptionInfo] = None
    venue: Venue = None
    size_mode: int = None
    sell_delta: float = None

class StrategySell(Strategy):
    def __init__(self, config: StrategySellConfig):
        super().__init__(config)
        self.spot = config.spot
        self.infos = config.infos
        self.size_mode = config.size_mode
        self.id_inst = { x.id: x for x in config.infos }

        self.hold_id = None
        self.hold_from: datetime.datetime = None
        self.hold_action = None
        
        info_list = [{
            'inst': x.inst,
            'expiry_date': x.expiry_date,
            'first_day': x.first_day,
            'last_day': x.last_day,
            'cp': x.cp,
            'strike': x.strike } for x in self.infos.values()]
        print(info_list)
        self.df_info = pd.DataFrame(info_list)
        
    def get_cash(self):    
        cash = self.portfolio.account(self.config.venue).balance().total.as_double()
        return cash
    
    def get_net_worth(self):
        cash = self.portfolio.account(self.config.venue).balance().total.as_double()
        pos_value = 0
        if self.hold_id is not None:
            pos_value = self.portfolio.net_exposure(self.hold_id).as_double()
        sum = cash + pos_value
        self.log.info(f"account: cash={cash}, pos={pos_value}, sum={sum}")
        return sum
    
    def on_start(self):
        self.log.info('on_start: subscribe all contracts.')
        self.subscribe_quote_ticks(self.spot.id)
        for x in self.infos:
            self.subscribe_quote_ticks(x.id)
    
    def on_quote_tick(self, tick: MyQuoteTick):
        # self.log.info(repr(tick))
        if tick.instrument_id == self.spot.id:
            self.on_spot_tick(tick)
        else:
            self.on_option_tick(tick)
    
    def on_spot_tick(self, tick: MyQuoteTick):
        self.log.info(f'net_worth={self.get_net_worth()}')
        spot_price = tick.ask_price
        spot_action = tick.action
        if spot_action is None:
            self.log.info(f"spot action is none.")
            return
        if self.hold_action == spot_action:
            self.log.info(f"hold action is same, skip this.")
            return
        self.log.info(f'spot price={spot_price}, action={spot_action}')
        self.close_all()
        if spot_action == 0:
            self.log.info(f"spot action is 0, skip this.")
            return
        now = self.clock.utc_now() 
        self.log.info(f"now={now}")
        if (now.hour == 6 and now.minute > 30) or now.hour > 6:
            self.log.info(f"now is after 14:30, skip this.")
            return
        if (now.hour == 1 and now.minute < 40):
            self.log.info(f"now is before 9:40, skip this.")
            return

        avail_opts = self.pick_available_options(now)
        # Sell Options
        cp = -1 if spot_action == 1 else -1
        pick_opt = self.pick_option_with_delta(avail_opts, cp * self.config.sell_delta)
        if pick_opt is None:
            self.log.info("cannot pick opt, skip this.")
            return

        # Make order 
        inst = pick_opt['inst']
        sell_tick = self.cache.quote_tick(inst.id)
        if sell_tick is None:
            self.log.info("cannot read opt md, skip this.")
            return
        if sell_tick.bid_price == 0:
            self.log.info(f"last ask_price is zero, skip this, price={repr(sell_tick)}")
            return

        sell_margin = self.calc_option_margin(inst.id)
        
        if self.size_mode == 1:
            # mode 1
            sell_size = self.get_cash() / sell_margin // 10000 * 10000
        elif self.size_mode == 2:
            # mode 2
            sell_size = (1_000_000 / sell_margin) // 10000 * 10000
        elif self.size_mode == 3:
            # mode 3
            sell_size = 500_000
        elif self.size_mode == 4:
            # mode 4
            sell_size = min(500_000, (1_000_000 / sell_margin) // 10000 * 10000)
        else:
            raise RuntimeError(f"unknown size mode={self.size_mode}")

        bidp = sell_tick.bid_price.as_double()
        self.log.info(f"pick opt={pick_opt['inst'].id}, bid_price={bidp}, size={sell_size}")
        self.hold_id = inst.id
        self.hold_from = now
        self.hold_action = spot_action
        order = self.order_factory.market(
            instrument_id=inst.id,
            order_side=OrderSide.SELL,
            quantity=inst.make_qty(sell_size),
            time_in_force=TimeInForce.FOK,
        )
        self.submit_order(order)

    def on_option_tick(self, tick: MyQuoteTick):
        now = self.clock.utc_now() 
        tick_id = tick.instrument_id
        opt_info: OptionInfo = self.infos[self.id_inst[tick_id]]
        if now.date() == opt_info.last_day:
            self.close_all()
        if self.size_mode == 1:
            if now.hour == 6 and now.minute > 40:
                if self.hold_id is not None:
                    self.log.info(f"now is after 14:40, close daliy option position.")
                    self.close_all()

    def pick_available_options(self, now: datetime.datetime):
        now_date = now.date()
        days = self.df_info['expiry_date']
        select_day = days[days > now_date].min()
        available_opts = self.df_info[
            (self.df_info['expiry_date'] == select_day)
            & (self.df_info['first_day'] <= now_date)
            & (self.df_info['last_day'] > now_date)]
        return available_opts

    def pick_option_with_delta(self, avail: pd.DataFrame, target_delta: float):
        if target_delta == 0 or avail.shape[0] == 0:
            return None
        if target_delta > 0:
            avail = avail.loc[avail['cp'] == 1].copy()
        else:
            avail = avail.loc[avail['cp'] == -1].copy()
        
        def read_delta(inst: Instrument) -> float:
            quote: MyQuoteTick = self.cache.quote_tick(inst.id)
            if quote is None:
                return None
            if quote.delta == 0:
                return None
            return quote.delta

        avail['delta'] = avail['inst'].apply(read_delta)
        avail = avail.loc[~avail['delta'].isna()]
        avail = avail.sort_values(by='delta', key=lambda x: abs(x - target_delta))
        if (avail.shape[0] == 0):
            return None
        return avail.iloc[0]

    def close_all(self):
        if self.hold_id is None:
            return
        self.close_all_positions(self.hold_id)
        self.hold_id = None
        self.hold_action = 0

    def calc_option_margin(self, inst_id):
        # 计算期权的保证金
        opt_info: OptionInfo = self.infos[self.id_inst[inst_id]]
        spot_price = self.cache.quote_tick(self.spot.id).bid_price.as_double()
        opt_price = self.cache.quote_tick(inst_id).bid_price.as_double()
        strike = opt_info.strike
        return self.calc_etf_option_margin(opt_info.cp, strike, opt_price, spot_price)

    # 估算每份期权的保证金，结果 * MULTIPLIER 之后是每手的保证金。
    @staticmethod
    def calc_etf_option_margin(callput, strike, optprice, spotprice):
        oom = 0
        if callput == 1 and strike > spotprice:
            # call out of money
            oom = strike - spotprice
        elif callput == -1 and strike < spotprice:
            # put out of money
            oom = spotprice - strike
        res1 = optprice + max(0.12 * spotprice - oom, 0.07 * spotprice)
        if callput == 1:
            return res1
        else:
            return min(res1, strike)
    