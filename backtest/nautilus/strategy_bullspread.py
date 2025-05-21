"""
牛市价差策略
做多采用认沽价差做多 buy P-0.4 sell P-0.6
做空采用认购价差做空 buy C+0.4 sell C+0.6
至于具体采用多少的 delta 这个可以配置。
"""

import pandas as pd
import datetime

from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.instruments import Instrument, Equity
from nautilus_trader.model.identifiers import Venue, InstrumentId
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import OrderSide

from backtest.nautilus.data_types import MyQuoteTick, OptionInfo

class StrategyBullSpreadConfig(StrategyConfig, frozen=True):
    mode: int = None
    spot: Instrument = None
    infos: dict[Instrument, OptionInfo] = None
    venue: Venue = None
    # 一次仓位保持的时间
    hold_days_limit: int = None

    long_buy_delta: float = None
    long_sell_delta: float = None
    short_buy_delta: float = None
    short_sell_delta: float = None

class StrategyBullSpread(Strategy):
    """
    牛市价差策略
    这个策略的输入数据需要包含多空信号，
    策略只是根据多空信号产生期权结构持仓。
    策略自己不计算多空信号。
    这里每次开仓固定是 10 手。
    """
    def __init__(self, config: StrategyBullSpreadConfig):
        super().__init__(config)
        self.id_inst = { x.id: x for x in config.infos }
        # self.config: StrategyBullSpreadConfig = config

        self.hold_id_list: list[InstrumentId] = None
        self.hold_from: datetime.datetime = None
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
        cash = self.portfolio.account(self.config.venue).balance().total.as_double()
        pos_value = 0
        if self.hold_id_list is not None:
            for id in self.hold_id_list:
                pos_value = self.portfolio.net_exposure(id).as_double()
        sum = cash + pos_value
        self.log.info(f"account: cash={cash}, pos={pos_value}, sum={sum}")
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
        spot_action = tick.action
        if spot_action is None:
            self.log.info(f"spot action is none.")
            return
        self.log.info(f'spot price={spot_price}, action={spot_action}')

        # 每次收到 action 都关闭之前的仓位完全重开
        self.close_all()
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
        # 开 10 手
        open_size = 10_0000
        self.hold_id_list = [buy_opt.id, sell_opt.id]
        self.hold_from = now
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
        if (self.hold_id_list is not None
                and tick_id in self.hold_id_list
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
        if self.hold_id_list is None:
            return
        for id in self.hold_id_list:
            self.close_all_positions(id)
        self.hold_id_list = None
    
    def on_stop(self):
        pass
    
    def on_reset(self):
        pass

