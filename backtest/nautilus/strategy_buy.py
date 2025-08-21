import pandas as pd
import datetime
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.instruments import Instrument, Equity
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import OrderSide

from backtest.nautilus.data_types import MyQuoteTick, OptionInfo

class StrategyBuyConfig(StrategyConfig, frozen=True):
    spot: Instrument = None
    infos: dict[Instrument, OptionInfo] = None
    venue: Venue = None
    size_mode: int = None
    hold_days_limit: int = None

class StrategyBuy(Strategy):
    def __init__(self, config: StrategyBuyConfig):
        super().__init__(config)
        self.spot = config.spot
        self.infos = config.infos
        self.size_mode = config.size_mode
        self.id_inst = { x.id: x for x in config.infos }

        self.hold_id = None
        self.hold_from: datetime.datetime = None
        self.hold_action = 0
        
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

        if self.hold_action * spot_action <= 0:
            # if hold action is different from spot action, close all positions.
            self.log.info(f"hold action is different from spot action, close all positions,"
                          f"hold_action={self.hold_action}, spot_action={spot_action}")
            self.close_all()
        if spot_action == 0:
            self.log.info(f"spot action is 0, skip this.")
            return

        now = self.clock.utc_now() 
        self.log.info(f"now={now}")
        # if (now.hour == 6 and now.minute > 30) or now.hour > 6:
        #     self.log.info(f"now is after 14:30, skip this.")
        #     return
        # if (now.hour == 1 and now.minute < 40):
        #     self.log.info(f"now is before 9:40, skip this.")
        #     return

        # Buy Options
        if self.hold_id is not None:
            pick_opt = self.infos[self.id_inst[self.hold_id]]
            inst = pick_opt.inst
            if pick_opt.cp * spot_action < 0:
                self.log.info(f"hold option is not suitable, close all positions.")
                self.close_all()
                return

        if self.hold_id is None:
            avail_opts = self.pick_available_options(now)
            buy_cp = 1 if spot_action > 0 else -1
            pick_opt = self.pick_option_with_delta(avail_opts, buy_cp * 0.38)
            if pick_opt is None:
                self.log.info("cannot pick opt, skip this.")
                return
            inst: Instrument = pick_opt['inst']
            last_quote = self.cache.quote_tick(inst.id)
            askp = last_quote.ask_price.as_double()
            if last_quote is None:
                self.log.info("cannot read opt md, skip this.")
                return
            if last_quote.ask_price == 0:
                self.log.info(f"last ask_price is zero, skip this, price={repr(last_quote)}")
                return
            full_size = (10_000 / askp) // 10000 * 10000
            self.full_size = full_size

        hold_size = self.full_size * abs(spot_action) // 10000 * 10000
        if self.hold_id is not None:
            trade_size = hold_size - self.hold_size
        else:
            trade_size = hold_size

        self.log.info(f"pick opt={inst.id}, hold_size={hold_size}, trade_size={trade_size}")
        if abs(trade_size) < 10000:
            self.log.info(f"trade size is too small, skip this, size={trade_size}")
            return

        self.hold_id = inst.id
        self.hold_from = now
        self.hold_action = spot_action
        self.hold_size = hold_size
        order = self.order_factory.market(
            instrument_id=inst.id,
            order_side=OrderSide.BUY if trade_size > 0 else OrderSide.SELL,
            quantity=inst.make_qty(abs(trade_size)),
            time_in_force=TimeInForce.FOK,
        )
        self.submit_order(order)

    def on_option_tick(self, tick: MyQuoteTick):
        now = self.clock.utc_now() 
        tick_id = tick.instrument_id
        opt_info: OptionInfo = self.infos[self.id_inst[tick_id]]
        if now.date() == opt_info.last_day:
            self.close_all()

        if self.size_mode % 2 == 1:
            if now.hour == 6 and now.minute > 40:
                if self.hold_id is not None:
                    self.log.info(f"now is after 14:40, close daliy option position.")
                    self.close_all()

        if (self.hold_id is not None and self.hold_id == tick_id
                and self.hold_from + datetime.timedelta(days=self.config.hold_days_limit) < now):
            self.log.info("close because hold time limit")
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

    def pick_atm_option(self, avail, spot_price, cp):
        if avail.shape[0] == 0:
            return None
        avail = avail[avail['cp'] == cp]
        avail = avail.sort_values(by='strike', key=lambda st: abs(st - spot_price))
        if avail.shape[0] == 0:
            return None
        return avail.iloc[0]

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
        self.hold_size = 0
        self.full_size = 0

