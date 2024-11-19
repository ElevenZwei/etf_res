
import math
import pandas as pd
import datetime
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.instruments import Instrument, Equity
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import OrderSide

from data_types import MyQuoteTick, OptionInfo

class StrategyBuyConfig(StrategyConfig, frozen=True):
    spot: Instrument = None
    infos: dict[Instrument, OptionInfo] = None
    venue: Venue = None
    size_mode: int = None
    hold_days_limit: int = None
    impv_min: float = None
    impv_max: float = None

class StrategyBuy(Strategy):
    def __init__(self, config: StrategyBuyConfig):
        super().__init__(config)
        self.spot = config.spot
        self.infos = config.infos
        self.size_mode = config.size_mode
        self.id_inst = { x.id: x for x in config.infos }

        self.hold_id = None
        self.hold_from: datetime.datetime = None
        
        info_list = [{
            'inst': x.inst,
            'expiry_date': x.expiry_date,
            'first_day': x.first_day,
            'last_day': x.last_day,
            'cp': x.cp,
            'strike': x.strike } for x in self.infos.values()]
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
        self.log.info(f'spot price={spot_price}, action={spot_action}')
        self.close_all()

        now = self.clock.utc_now() 
        avail_opts = self.pick_available_options(now)
        # self.log.info(f"avail_opts: {repr(avail_opts[['first_day', 'last_day', 'expiry_date']])}")
        # Buy Options
        cp = 1 if spot_action == 1 else -1
        pick_opt = self.pick_atm_option(avail_opts, spot_price, cp)
        other_opt = self.pick_atm_option(avail_opts, spot_price, -cp)
        if pick_opt is None:
            self.log.info("cannot pick opt, skip this.")
            return

        impv_ratio = 1

        # pick_quote = self.cache.quote_tick(pick_opt['inst'].id)
        # if pick_quote is None:
        #     self.log.info("quote is none.")
        #     return
        # other_quote = self.cache.quote_tick(other_opt['inst'].id)
        # atm_impv = (pick_quote.impv + other_quote.impv) / 2
        # impv_amp = (self.config.impv_max - self.config.impv_min)
        # impv_ratio = max(min((atm_impv - self.config.impv_min) / impv_amp, 1), 0)
        # impv_ratio = round(impv_ratio, 1)
        # self.log.info(f'atm_impv={atm_impv}, impv_ratio={impv_ratio}')
        # if impv_ratio == 0:
        #     return


        # Make order 
        inst = pick_opt['inst']
        last_quote = self.cache.quote_tick(inst.id)
        if last_quote is None:
            self.log.info("cannot read opt md, skip this.")
            return
        if last_quote.ask_price == 0:
            self.log.info(f"last ask_price is zero, skip this, price={repr(last_quote)}")
            return

        askp = last_quote.ask_price.as_double()
        if self.size_mode == 1:
            # mode 1
            buy_size = self.get_cash() * 0.005 / askp // 10000 * 10000
        elif self.size_mode == 2:
            # mode 2
            buy_size = (5_000 / askp) // 10000 * 10000
        elif self.size_mode == -2:
            # mode -2
            buy_size = (5_000 / last_quote.ask_price) // 10000 * 10000
        elif self.size_mode == 3:
            # mode 3
            buy_size = 300_000
        elif self.size_mode == 4:
            # mode 4
            buy_size = min(500_000, (5_000 / askp) // 10000 * 10000)
        elif self.size_mode == 5:
            # mode 5
            buy_size = (5_000 / max(0.01, askp)) // 10000 * 10000
        elif self.size_mode == 6:
            # 模拟定点数的操作
            if askp < 0.01:
                askp = 0.01
            askp = round(askp * 100) / 100
            buy_size = (5000 / askp) // 10000 * 10000
        elif self.size_mode == 7:
            if askp < 0.01:
                askp = 0.01
            askp = math.floor(askp * 100) / 100
            buy_size = (5000 / askp) // 10000 * 10000
        elif self.size_mode == 8:
            if askp < 0.01:
                askp = 0.01
            askp = math.ceil(askp * 100) / 100
            buy_size = (5000 / askp) // 10000 * 10000
        elif self.size_mode == 9:
            if askp < 0.01:
                askp = 0.01
            askp = round(askp * 100 + 0.2) / 100
            buy_size = (5000 / askp) // 10000 * 10000
        elif self.size_mode == 10:
            if askp < 0.01:
                askp = 0.01
            askp = round(askp * 100 - 0.2) / 100
            buy_size = (5000 / askp) // 10000 * 10000
        else:
            raise RuntimeError(f"unknown size mode={self.size_mode}")

        self.log.info(f"pick opt={pick_opt['inst'].id}, ask_price={askp}, size={buy_size}")
        self.hold_id = inst.id
        self.hold_from = now
        order = self.order_factory.market(
            instrument_id=inst.id,
            order_side=OrderSide.BUY,
            quantity=inst.make_qty(buy_size * impv_ratio),
            time_in_force=TimeInForce.FOK,
        )
        self.submit_order(order)

    def on_option_tick(self, tick: MyQuoteTick):
        now = self.clock.utc_now() 
        tick_id = tick.instrument_id
        opt_info: OptionInfo = self.infos[self.id_inst[tick_id]]
        if now.date() == opt_info.last_day:
            self.close_all()
            # self.close_all_positions(tick.instrument_id)
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
    
    def close_all(self):
        if self.hold_id is None:
            return
        self.close_all_positions(self.hold_id)
        self.hold_id = None

