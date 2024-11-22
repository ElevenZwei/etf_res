# 整理一下我载入回测使用的数据类型。

from decimal import Decimal
import tqdm
import pandas as pd

from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.objects import Money, Price, Currency, Quantity
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.persistence.loaders import CSVTickDataLoader, CSVBarDataLoader
from nautilus_trader.model.identifiers import InstrumentId, Symbol
from nautilus_trader.model.instruments import Instrument, Equity
from nautilus_trader.model.currencies import CNY
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.model.enums import AccountType, OmsType, TimeInForce

from backtest.config import DATA_DIR

class MyQuoteTick(QuoteTick):
    def set_greeks(self, impv, delta):
        self.impv = impv
        self.delta = delta
    def set_action(self, action):
        self.action = action
    def set_oi(self, oicp):
        self.oicp = oicp

class OptionInfo:
    def __init__(self, inst, cp, expiry_date, first_day, last_day, strike):
        self.inst = inst
        self.cp = cp
        self.expiry_date = expiry_date
        self.first_day = first_day
        self.last_day = last_day
        self.strike = strike

def prepare_venue(engine, venue_name):
    fill_model = FillModel(
        prob_fill_on_limit=0.2,
        prob_fill_on_stop=0.95,
        prob_slippage=0,
        random_seed=42,
    )
    ven = Venue(venue_name)
    engine.add_venue(
        venue=ven,
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        base_currency=CNY,
        starting_balances=[Money(1_000_000, CNY)],
        # fill_model=fill_model,
    )
    return ven
        
def df_to_my_quote(df, inst):
    res = []
    df['epoch_ns'] = df.index.astype('int64')
    df = df.sort_values(by='epoch_ns')
    code = df['code'].iloc[0]
    df.to_csv(f'{DATA_DIR}/tmp/{code}_2.csv')
    for tup in df.itertuples():
        tick = MyQuoteTick(
            instrument_id=inst.id,
            bid_price=Price(tup.bid, 4),
            ask_price=Price(tup.ask, 4),
            bid_size=Quantity.from_int(1e9),
            ask_size=Quantity.from_int(1e9),
            ts_event=tup.epoch_ns,
            ts_init=tup.epoch_ns,
        )
        if hasattr(tup, 'impv'):
            tick.set_greeks(tup.impv, tup.delta)
        if hasattr(tup, 'action'):
            tick.set_action(tup.action)
        if hasattr(tup, 'oicp'):
            tick.set_oi(tup.oicp)
        res.append(tick)
    return res

def prepare_spot_quote(csv_fpath, engine, venue, bgdt, eddt):
    # df = CSVTickDataLoader.load('../input/spot_sig_159915.csv', 'dt')
    # df = CSVTickDataLoader.load('../input/oi_signal_159915_act_changes.csv', 'dt')
    df = CSVTickDataLoader.load(csv_fpath, 'dt')
    se_dt = df.index.to_series().dt.date
    df = df[(se_dt >= bgdt) & (se_dt < eddt)]
    codes = df['code'].unique()
    assert(len(codes) == 1)
    spot = codes[0]
    df.loc[:, 'ask'] = df['price']
    df.loc[:, 'bid'] = df['price']
    spot_symbol = Symbol(spot)
    inst = Equity(
        instrument_id=InstrumentId(symbol=spot_symbol, venue=venue),
        raw_symbol=spot_symbol,
        currency=CNY,
        price_precision=4,
        price_increment=Price.from_str("0.0001"),
        lot_size=Quantity.from_int(10000),
        margin_init=Decimal("1"),
        margin_maint=Decimal("1"),
        ts_event=0,
        ts_init=0,
    )
    engine.add_instrument(inst)
    engine.add_data(df_to_my_quote(df, inst))
    return inst

def prepare_option_quote(csv_fpath, engine, venue, bgdt, eddt):
    # df = CSVTickDataLoader.load('../input/options_159915_minute_data.csv', 'dt')
    # df = CSVTickDataLoader.load('../input/options_data.csv', 'dt')
    # df = CSVTickDataLoader.load('../input/options_159915_clip.csv', 'dt')
    # df = CSVTickDataLoader.load('../input/tl_greeks_159915_all.csv', 'dt')
    # df = CSVTickDataLoader.load('../input/tl_greeks_159915_all_fixed.csv', 'dt')
    # df = CSVTickDataLoader.load('../input/tl_greeks_159915_clip_fixed.csv', 'dt')
    df = CSVTickDataLoader.load(csv_fpath, 'dt')
    se_dt = df.index.to_series().dt.date
    df = df[(se_dt >= bgdt) & (se_dt < eddt)]
    # df.to_csv('../input/tl_options_159915_clip.csv')
    codes = df['code'].unique()
    infos = {}
    for code in tqdm.tqdm(codes):
        df_clip = df[df['code'] == code].copy()
        df_clip.loc[:, 'ask']= df_clip['closep']
        df_clip.loc[:, 'bid'] = df_clip['closep']
        id = df_clip['tradecode'].iloc[0]
        # print(id)
        cp = 1 if 'C2' in id else -1
        opt_symbol = Symbol(id)
        inst = Equity(
            instrument_id=InstrumentId(symbol=opt_symbol, venue=venue),
            raw_symbol=opt_symbol,
            currency=CNY,
            price_precision=4,
            price_increment=Price.from_str("0.0001"),
            lot_size=Quantity.from_int(10000),
            margin_init=Decimal("1"),
            margin_maint=Decimal("1"),
            ts_event=0,
            ts_init=0
        )
        engine.add_instrument(inst)
        engine.add_data(df_to_my_quote(df_clip, inst))
        infos[inst] = OptionInfo(
                inst=inst,
                cp=cp,
                expiry_date=pd.to_datetime(df_clip['expirydate'].iloc[0]).date(),
                first_day=df_clip.index.min().date(),
                last_day=df_clip.index.max().date(),
                strike=df_clip['strike'].iloc[0])
    return infos