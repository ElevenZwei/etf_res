"""
牛市价差策略
做多采用认沽价差做多 buy P-0.4 sell P-0.6
做空采用认购价差做空 buy C+0.4 sell C+0.6
至于具体采用多少的 delta 这个可以配置。

这个策略需要采集更加原始的 spot 价格和 diff oi 的数据。
然后手动计算其中的保证金和开仓平仓的过程。

"""

import pandas as pd
import datetime

from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.identifiers import Venue, InstrumentId
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import OrderSide

from data_types import MyQuoteTick, OptionInfo

class StrategyBullSpreadConfig(StrategyConfig, frozen=True):
    mode: int = None
    spot: Instrument = None
    infos: dict[Instrument, OptionInfo] = None
    venue: Venue = None

    long_buy_delta: float = None
    long_sell_delta: float = None
    short_buy_delta: float = None
    short_sell_delta: float = None
    open_amount: int = None
    
class HoldInfo(frozen=True):
    buy_opt: Instrument = None
    sell_opt: Instrument = None
    open_amount: int = None
    open_from: datetime.datetime = None

class StrategyBullSpread(Strategy):
    def __init__(self, config: StrategyBullSpreadConfig):
        super().__init__(config)
        self.id_inst = { x.id: x for x in config.infos }

        self.holds: list[HoldInfo] = None
        self.df_info = pd.DataFrame([{
            'inst': x.inst,
            'expiry_date': x.expiry_date,
            'first_day': x.first_day,
            'last_day': x.last_day,
            'cp': x.cp,
            'strike': x.strike
        } for x in config.infos.values()])
        self.prev_now = None

    def get_cash(self):    
        cash = self.portfolio.account(self.config.venue).balance().total.as_double()
        return cash
    
    def get_net_worth(self):
        cash = self.get_cash()
        return cash

    def on_start(self):
        self.log.info('on_start: subscribe all contracts.')
        self.subscribe_quote_ticks(self.config.spot.id)
        for id in self.id_inst.keys():
            self.subscribe_quote_ticks(id)
        
    def on_quote_tick(self, tick: MyQuoteTick):
        # self.log.info(repr(tick))
        now = self.clock.utc_now() 
        if self.prev_now is None or self.prev_now.date() != now.date():
            # a new day
            self.log.info(f'net_worth={self.get_net_worth()}')
        self.prev_now = now

        if tick.instrument_id == self.config.spot.id:
            self.on_spot_tick(tick)
        else:
            self.on_option_tick(tick)

    def on_spot_tick(self, tick: MyQuoteTick):
        spot_price = tick.ask_price

        # get spot action

        spot_action = tick.action
        self.log.info(f'spot price={spot_price}, action={spot_action}')
        if spot_action is None:
            self.log.info(f"spot action is none.")
            return
        if spot_action == 0:
            return

        # 找到最近的 option 数值开仓
        if spot_action == 1:
            buy_delta = self.config.long_buy_delta
            sell_delta = self.config.long_sell_delta
        elif spot_action == -1:
            buy_delta = self.config.short_buy_delta
            sell_delta = self.config.short_sell_delta
        else:
            self.log.error(f"unknown spot action={spot_action}")
            return

        now = self.clock.utc_now() 
        avail = self.pick_available_options(now)
        buy_line = self.pick_option_with_delta(avail, buy_delta)
        sell_line = self.pick_option_with_delta(avail, sell_delta)
        if buy_line is None or sell_line is None:
            self.log.info(f"cannot pick option.")
            return
        buy_opt = buy_line['inst']
        sell_opt = sell_line['inst']
        self.log.info(f"pick buy_opt={buy_opt.id}, buy_delta={buy_line['delta']}"
                      f", sell_opt={sell_opt.id}, sell_delta={sell_line['delta']}")

        open_size = self.config.open_amount * 2
        self.holds.append(HoldInfo(
            buy_opt=buy_opt,
            sell_opt=sell_opt,
            open_amount=open_size,
            open_from=now,
        ))
        buy_order = self.order_factory.market(
            instrument_id=buy_opt.id,
            order_side=OrderSide.BUY,
            quantity=buy_opt.make_qty(open_size),
            time_in_force=TimeInForce.FOK,
        )
        sell_order = self.order_factory.market(
            instrument_id=sell_opt.id,
            order_side=OrderSide.SELL,
            quantity=sell_opt.make_qty(open_size),
            time_in_force=TimeInForce.FOK,
        )
        self.submit_order(buy_order)
        self.submit_order(sell_order)

    def on_option_tick(self, tick: MyQuoteTick):
        now = self.clock.utc_now() 
        tick_id = tick.instrument_id
        opt_info: OptionInfo = self.config.infos[self.id_inst[tick_id]]
        # 期权到期或者持有时间超出限制都是全部清空。
        if now.date() == opt_info.last_day:
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
        # avail = avail.loc[avail['delta'].abs() < abs(target_delta)]
        self.log.info(repr(avail))
        avail = avail.sort_values(by='delta', key=lambda x: abs(x - target_delta))
        return avail.iloc[0]

    def close_all(self):
        for holdinfo in self.holds:
            self.close_all_positions(holdinfo.buy_opt)
            self.close_all_positions(holdinfo.sell_opt)
        self.holds = []
        
    def calc_item_margin(self):
        pass
        
    
    def on_stop(self):
        pass
    
    def on_reset(self):
        pass

