"""
牛市价差策略
做多采用认沽价差做多 buy P-0.4 sell P-0.6
做空采用认购价差做空 buy C+0.4 sell C+0.6
至于具体采用多少的 delta 这个可以配置。

这个策略需要采集更加原始的 spot 价格和 diff oi 的数据。
然后手动计算其中的保证金和开仓平仓的过程。

"""

from dataclasses import dataclass
import pandas as pd
import datetime

from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.identifiers import Venue, InstrumentId
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import OrderSide

from backtest.nautilus.data_types import MyQuoteTick, OptionInfo

class StrategyBullSpread2Config(StrategyConfig, frozen=True):
    mode: int = None
    spot: Instrument = None
    infos: dict[Instrument, OptionInfo] = None
    venue: Venue = None

    long_buy_delta: float = None
    long_sell_delta: float = None
    short_buy_delta: float = None
    short_sell_delta: float = None
    open_amount: int = None

    base_oi_interval: int = 23 * 60
    diff_oi_threshold: int = 4000
    cash_usage: float = 100_000

@dataclass(frozen=True)
class HoldInfo():
    buy_opt: Instrument = None
    sell_opt: Instrument = None
    open_amount: int = None
    open_from: datetime.datetime = None

class StrategyBullSpread2(Strategy):
    def __init__(self, config: StrategyBullSpread2Config):
        super().__init__(config)
        self.id_inst = { x.id: x for x in config.infos }
        self.id_info = { x.id: self.config.infos[x] for x in config.infos}
        self.dt1970: datetime.datetime = datetime.datetime(1970, 1, 1,
                tzinfo=datetime.timezone(datetime.timedelta(hours=8)));

        self.holds: list[HoldInfo] = []
        self.holds_dir = 0
        self.df_info = pd.DataFrame([{
            'inst': x.inst,
            'expiry_date': x.expiry_date,
            'first_day': x.first_day,
            'last_day': x.last_day,
            'cp': x.cp,
            'strike': x.strike
        } for x in config.infos.values()])

        self.prev_now = self.dt1970
        self.enable_trade = True
        self.oi_base = 0
        self.oi_base_from = self.dt1970
        self.oi_flag = 0
        self.oi_flag_from = self.dt1970
        self.oi_delta = 0

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
            self.on_date_change()
        self.prev_now = now

        if tick.instrument_id == self.config.spot.id:
            self.on_spot_tick(tick)
        else:
            self.on_option_tick(tick)

    def on_date_change(self):
        total_margin = self.calc_total_margin()
        self.log.info(f'net_worth={self.get_net_worth()}, margin={total_margin}')
        # clear trade flags
        now = self.clock.utc_now() 
        self.enable_trade = True
        # refresh oi base every day
        self.oi_base = 0
        self.oi_base_from = self.dt1970

    def on_spot_tick(self, tick: MyQuoteTick):
        # get spot action
        self.check_oi_flag(tick)
        old_dir = self.holds_dir
        new_dir = self.calc_new_dir()
        spot_action = new_dir - old_dir
        if spot_action is None:
            self.log.info(f"spot action is none.")
            return
        if spot_action == 0:
            return
        self.log.info(f"spot action={spot_action}, old_dir={old_dir}, new_dir={new_dir}")

        # 找到最近的 option 数值开仓
        if spot_action > 0:
            buy_delta = self.config.long_buy_delta
            sell_delta = self.config.long_sell_delta
        elif spot_action < 0:
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
        open_size = self.config.open_amount * abs(spot_action)

        new_pair = HoldInfo(
            buy_opt=buy_opt,
            sell_opt=sell_opt,
            open_amount=open_size,
            open_from=now,
        )
        new_pair_margin = self.calc_pair_margin(new_pair)
        total_margin = self.calc_total_margin()
        if total_margin + new_pair_margin > self.config.cash_usage:
            need_remove = total_margin - (self.config.cash_usage - new_pair_margin)
            if not self.compress_to_remove(need_remove, spot_action > 0):
                self.log.info("cannot compress margin")
        
        self.holds.append(new_pair)
        self.holds_dir = new_dir
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
        opt_info: OptionInfo = self.id_info[tick_id]
        # 期权到期或者持有时间超出限制都是全部清空。
        if now.date() == opt_info.last_day:
            self.close_all()
            self.enable_trade = True

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
        # self.log.info(repr(avail))
        avail = avail.sort_values(by='delta', key=lambda x: abs(x - target_delta))
        return avail.iloc[0]

    def close_all(self):
        ids = set()
        for holdinfo in self.holds:
            ids.add(holdinfo.buy_opt.id)
            ids.add(holdinfo.sell_opt.id)
        self.log.info(f"close all ids: {ids}")
        for id in ids:
            self.close_all_positions(id)
        self.holds = []
    
    def close_hold_pair(self, pair: HoldInfo):
        sell_order = self.order_factory.market(
            instrument_id=pair.buy_opt.id,
            order_side=OrderSide.SELL,
            quantity=pair.buy_opt.make_qty(pair.open_amount),
            time_in_force=TimeInForce.GTC,
        )
        buy_order = self.order_factory.market(
            instrument_id=pair.sell_opt.id,
            order_side=OrderSide.BUY,
            quantity=pair.sell_opt.make_qty(pair.open_amount),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(sell_order)
        self.submit_order(buy_order)
        self.holds.remove(pair)
        pass
    
    def check_oi_flag(self, spot_tick: MyQuoteTick):
        now = self.clock.utc_now() 
        if (self.oi_base_from + datetime.timedelta(minutes=self.config.base_oi_interval)
                <= now):
            self.oi_base = spot_tick.oicp
            self.oi_base_from = now
            self.log.info(f"set new oi base={self.oi_base}")
        oi_diff = spot_tick.oicp - self.oi_base
        oi_dir = 0
        if oi_diff > self.config.diff_oi_threshold:
            oi_dir = -1
        elif oi_diff < -1 * self.config.diff_oi_threshold:
            oi_dir = 1
        
        # self.flag_oi will refresh whenever oi_diff value goes up or down.
        if oi_dir != self.oi_flag:
            self.oi_flag = oi_dir
            self.oi_flag_from = now
            self.log.info(f"oi_flag change to flag={self.oi_flag}, cp_diff={oi_diff}")
        
        if (self.oi_flag != 0 and self.oi_delta != self.oi_flag
                and self.oi_flag_from + datetime.timedelta(minutes=2) <= now):
            self.oi_delta = self.oi_flag
            self.log.info(f"set new oi delta dir={self.oi_delta}")

    def calc_new_dir(self):
        return self.oi_delta
        
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
        
    def calc_pair_margin(self, hold: HoldInfo):
        buy_tick: MyQuoteTick = self.cache.quote_tick(hold.buy_opt.id)
        buy_unit_margin = buy_tick.ask_price.as_double()
        sell_tick: MyQuoteTick = self.cache.quote_tick(hold.sell_opt.id)
        sell_info: OptionInfo = self.id_info[hold.sell_opt.id]
        spot_tick: MyQuoteTick = self.cache.quote_tick(self.config.spot.id)
        sell_unit_margin = self.calc_etf_option_margin(
                sell_info.cp, sell_info.strike,
                sell_tick.ask_price.as_double(), spot_tick.ask_price.as_double())
        res = (buy_unit_margin + abs(sell_unit_margin)) * hold.open_amount
        self.log.info(f"pair margin {hold.buy_opt.id} {hold.sell_opt.id} = {res}")
        return res

    def calc_total_margin(self):
        sum = 0 
        for hold in self.holds:
            sum += self.calc_pair_margin(hold)
        return sum
    
    def compress_to_remove(self, to_relieve: float, close_pos_delta: bool) -> bool:
        self.log.info(f"need to relieve={to_relieve}, dir={close_pos_delta}")
        if to_relieve <= 0:
            return True
        if len(self.holds) == 0:
            self.log.info("hold empty cannot compress")
            return False

        # 优先平需要的方向里面 delta 差值最大的组合
        def sort_delta(hold: HoldInfo):
            buy_tick: MyQuoteTick = self.cache.quote_tick(hold.buy_opt.id)
            sell_tick: MyQuoteTick = self.cache.quote_tick(hold.sell_opt.id)
            diff = buy_tick.delta - sell_tick.delta
            if (close_pos_delta and diff < 0) or (not close_pos_delta and diff > 0):
                return 100
            return -1 * abs(diff)

        self.holds.sort(key=sort_delta)
        close_pairs = []
        for pair in self.holds:
            pair_margin = self.calc_pair_margin(pair)
            close_pairs.append(pair)
            to_relieve -= pair_margin
            self.log.info(f"plan to close {pair.buy_opt.id}+{pair.sell_opt.id}, pair_m={pair_margin} left={to_relieve}")
            if to_relieve <= 0:
                break

        if to_relieve > 10:
            return False
        for pair in close_pairs:
            self.close_hold_pair(pair)
        return True
    
    def on_stop(self):
        pass
    
    def on_reset(self):
        pass

