# This file reads ratio_diff data from SQL table cpr.daily 
# and threshold data from table cpr.clip.
# Combine those two tables to get trade signal.
# Input: spotcode, date, method, variation, clip time interval, open close threshold.
# Output: Intra-day signal for the spotcode.

from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import time, date, datetime, timedelta
from typing import Callable, Dict, List
from itertools import product

import tqdm
import json
import numpy as np
import pandas as pd
import sqlalchemy as sa
from config import get_engine, upsert_on_conflict_skip

engine = get_engine()

DATASET_CACHE: Dict[str, int] = {}
def load_dataset_id(spotcode: str):
    if spotcode in DATASET_CACHE:
        return DATASET_CACHE[spotcode]

    query = sa.text("""
        select id from cpr.dataset
        where spotcode = :spotcode
        and expiry_priority = 1 and strike = 0
        limit 1
    """);
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'spotcode': spotcode,
        })
    if df.empty:
        return None
    DATASET_CACHE[spotcode] = df.iloc[0]['id']
    return DATASET_CACHE[spotcode]


def load_method_id(method: str, variation: str):
    query = sa.text("""
        select id from cpr.method
        where name = :method
        and variation = :variation
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'method': method,
            'variation': variation,
        })
    if df.empty:
        raise ValueError(f"No method found for method {method} and variation {variation}")
    return df.iloc[0]['id']


METHOD_CACHE: Dict[str, int] = {}
def load_method_id_with_cache(method: str, variation: str) -> int:
    """
    Load method ID with caching.
    """
    key = f"{method}-{variation}"
    if key in METHOD_CACHE:
        return METHOD_CACHE[key]
    method_id = load_method_id(method, variation)
    METHOD_CACHE[key] = method_id
    return method_id


def load_range_id(ti: time, d1: date, d2: date):
    query = sa.text("""
        select id from cpr.dt_range
        where t1 = :ti and t2 = :ti
        and d1 = :d1 and d2 = :d2
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {
            'd1': d1,
            'd2': d2,
            'ti': ti
        })
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    if df.empty:
        raise ValueError(f"No range found for time {ti} and dates {d1} to {d2}")
    return df.iloc[0]['id']


RANGE_CACHE: Dict[str, int] = {}
def load_range_id_with_cache(ti: time, d1: date, d2: date) -> int:
    """
    Load range ID with caching.
    """
    key = f"{ti}-{d1}-{d2}"
    if key in RANGE_CACHE:
        return RANGE_CACHE[key]
    range_id = load_range_id(ti, d1, d2)
    RANGE_CACHE[key] = range_id
    return range_id


def load_cpr_daily(dsid: int, dat: date):
    query = sa.text("""
        select * from cpr.daily
        where dataset_id = :dsid
        and dt > :dat and dt < :dat + interval '1 day'
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'dsid': str(dsid),
            'dat': (dat),
        })
    if df.empty:
        return None
        # raise ValueError(f"No daily data found for {spotcode} on {dat}")
    return df


CPR_DAILY_CACHE: Dict[str, pd.DataFrame] = {}
def load_cpr_daily_with_cache(dsid: int, dat: date) -> pd.DataFrame:
    """
    Load daily CPR data with caching.
    """
    key = f"{dsid}-{dat}"
    if key in CPR_DAILY_CACHE:
        return CPR_DAILY_CACHE[key]
    cpr_data = load_cpr_daily(dsid, dat)
    CPR_DAILY_CACHE[key] = cpr_data
    return cpr_data


def load_clip(ds_id: int, method_id: int, ti: time, d1: date, d2: date):
    range_id = load_range_id_with_cache(ti, d1, d2)
    # data column is a JSONB type, so we need to extract it
    query = sa.text("""
        select data from cpr.clip
        where dataset_id = :ds_id
        and dt_range_id = :range_id
        and method_id = :method_id
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'ds_id': str(ds_id),
            'method_id': str(method_id),
            'range_id': str(range_id),
        })
    if df.empty:
        raise ValueError(f"No clip data found for dataset {ds_id}, method {method_id}, time {ti}, dates {d1} to {d2}")
    data = df['data'].iloc[0]
    return data

CLIP_CACHE: Dict[str, pd.DataFrame] = {}
def load_clip_with_cache(
    ds_id: int, method_id: int, ti: time, d1: date, d2: date,
    ) -> pd.DataFrame:
    """
    Load clip data with caching.
    """
    key = f"{ds_id}-{method_id}-{ti}-{d1}-{d2}"
    if key in CLIP_CACHE:
        return CLIP_CACHE[key]
    clip_data = load_clip(ds_id, method_id, ti, d1, d2)
    CLIP_CACHE[key] = clip_data
    return clip_data

@dataclass(frozen=True)
class SignalArgs:
    method: str = None
    variation: str = None
    date_interval: int = 30
    # this example arg means
    # open long at zero+open=0.2, close long at 0.5,
    # open short at zero+open=0.8, close short at 0.5.
    arg_variation: str = 'default'
    zero_threshold: float = 0.5
    long_open_threshold: float = -0.3
    long_close_threshold: float = 0.0
    short_open_threshold: float = 0.3
    short_close_threshold: float = 0.0


def upload_trade_args(args: SignalArgs):
    method_id = load_method_id_with_cache(args.method, args.variation)
    query = sa.text("""
        select get_or_create_clip_trade_args(
            :method_id,
            :date_interval,
            :arg_variation,
            :args) as id;
    """)
    with engine.connect() as conn:
        res = conn.execute(query, {
            'method_id': str(method_id),
            'date_interval': args.date_interval,
            'arg_variation': args.arg_variation,
            'args': json.dumps(asdict(args))
        })
        conn.commit()
        df = pd.DataFrame(res.fetchall(), columns=res.keys())
    if df.empty:
        raise ValueError()
    return df.iloc[0]['id']


def signal_intra_day(spotcode: str, dat: date, args: SignalArgs):
    ds_id = load_dataset_id(spotcode)
    method_id = load_method_id_with_cache(args.method, args.variation)
    cpr = load_cpr_daily_with_cache(ds_id, dat)
    if cpr is None or cpr.empty:
        return None
    cpr = cpr.set_index('dt').sort_index()
    cpr = cpr[['ti', 'ratio_diff']].copy()

    # end date is last Friday before the given date
    ed = dat - timedelta(days=dat.weekday() + 3)
    bg = ed - timedelta(days=args.date_interval)
    # threshold position
    clip_index = np.arange(0, 10.05, 0.05)
    long_open_index = np.searchsorted(clip_index, args.zero_threshold + args.long_open_threshold)
    long_close_index = np.searchsorted(clip_index, args.zero_threshold + args.long_close_threshold)
    short_open_index = np.searchsorted(clip_index, args.zero_threshold + args.short_open_threshold)
    short_close_index = np.searchsorted(clip_index, args.zero_threshold + args.short_close_threshold)

    if abs(args.long_open_threshold) >= 1 and abs(args.short_open_threshold) >= 1:
        return None  # both long and short open thresholds are off

    if abs(args.long_open_threshold) >= 1:
        long_enable = False
        long_open_index = 0
    else:
        long_enable = True

    if abs(args.short_open_threshold) >= 1:
        short_enable = False
        short_open_index = 0
    else:
        short_enable = True

    last_position = 0.0
    cpr['signal'] = 'close'  # default signal
    cpr['position'] = 0.0  # default position
    cpr['is_trading'] = True  # assume trading is active
    for tup in cpr.itertuples():
    # for tup in tqdm.tqdm(cpr.itertuples(), total=len(cpr), desc='Processing signals', leave=False):
        ti = tup.ti
        if ti > time(14, 55):
            cpr.at[tup.Index, 'is_trading'] = False
            continue  # no clip data after 14:55
        if ti == time(11, 30):
            cpr.at[tup.Index, 'is_trading'] = False
            cpr.at[tup.Index, 'position'] = last_position
            continue

        clip = load_clip_with_cache(ds_id, method_id, ti, bg, ed)
        clip_diff = clip['ratio_diff']
        long_open = clip_diff[long_open_index] if long_enable else -1000.0
        long_close = clip_diff[long_close_index] if long_enable else 1000.0
        short_open = clip_diff[short_open_index] if short_enable else 1000.0
        short_close = clip_diff[short_close_index] if short_enable else -1000.0
        cpr.at[tup.Index, 'long_open'] = long_open
        cpr.at[tup.Index, 'long_close'] = long_close
        cpr.at[tup.Index, 'short_open'] = short_open
        cpr.at[tup.Index, 'short_close'] = short_close

        ratio_diff = tup.ratio_diff
        if ratio_diff <= long_open:
            signal = 'long_open'
        elif ratio_diff <= long_close:
            signal = 'long_hold'
        elif ratio_diff >= short_open:
            signal = 'short_open'
        elif ratio_diff >= short_close:
            signal = 'short_hold'
        else:
            signal = 'close'
        cpr.at[tup.Index, 'signal'] = signal

        if ti < time(9, 35) or () or ti > time(14, 54):
            cpr.at[tup.Index, 'is_trading'] = False
            continue
        if ti > time(11, 25) and ti < time(12, 0):
            cpr.at[tup.Index, 'is_trading'] = False
            cpr.at[tup.Index, 'position'] = last_position
            continue

        if last_position == 0.0:
            if signal in ['long_open']:
                last_position = 1.0
            elif signal in ['short_open']:
                last_position = -1.0
        elif last_position == 1.0:
            if signal in ['long_hold', 'long_open']:
                last_position = 1.0
            elif signal in ['short_open']:
                last_position = -1.0
            else:
                last_position = 0.0
        elif last_position == -1.0:
            if signal in ['short_hold', 'short_open']:
                last_position = -1.0
            elif signal in ['long_open']:
                last_position = 1.0
            else:
                last_position = 0.0
        cpr.at[tup.Index, 'position'] = last_position

        # print(f"Signal for {tup.Index} at {ti}: {cpr.at[tup.Index, 'signal']}")
        # print(f"Ratio diff: {ratio_diff}, Clip diff threshold: "
        #       f"{clip_diff[short_open_index]} (short open), "
        #       f"{clip_diff[short_close_index]} (short close)"
        #       f"{clip_diff[long_close_index]} (long close), "
        #       f"{clip_diff[long_open_index]} (long open), "
        #   )
        # print(f"Clip data: {clip['ratio_diff']}")
        # print(f"Position: {last_position}")
        # return

    cpr['spotcode'] = spotcode
    cpr = cpr.rename(columns={
        'ratio_diff': 'value',
        'signal': 'zone',
    }).drop(columns=['ti'])
    return cpr


def upload_trade(df: pd.DataFrame):
    # filter out rows which contains null values in any column
    df = df.dropna(how='any')
    df.to_sql('clip_trade_backtest', engine, schema='cpr',
            if_exists='append', index=False,
            method=upsert_on_conflict_skip,
            chunksize=1000)


def upload_trade_with_args(args: SignalArgs, df: pd.DataFrame):
    df = df.reset_index()
    trade_args_id = upload_trade_args(args)
    df['trade_args_id'] = trade_args_id
    spotcode = df['spotcode'].iloc[0]
    dataset_id = load_dataset_id(spotcode)
    df['dataset_id'] = dataset_id
    df = df[['dt', 'dataset_id', 'trade_args_id',
             'is_trading', 'zone', 'position', 'value',
            'long_open', 'long_close',
             'short_open', 'short_close']]
    # print(df.head())
    upload_trade(df)


def test_signal_intra_day():
    spotcode = '510500'
    dat = date(2025, 1, 2)
    args = SignalArgs(
        method='z_score',
        variation='default',
        date_interval=30,
        arg_variation='default',
        zero_threshold=0.5,
        # long_open_threshold=-0.3,
        long_open_threshold=-1000,
        long_close_threshold=0.0,
        short_open_threshold=0.3,
        short_close_threshold=0.0,
    )
    df = signal_intra_day(spotcode, dat, args)
    # print(df.head(40))
    print(df.tail(40))
    upload_trade_with_args(args, df)

method_list = [
    { "method": 'min_max', "variation": 'default' },
    { "method": 'min_max', "variation": 'p_05' },
    { "method": 'min_max', "variation": 'p_10' },
    { "method": 'percentile', "variation": 'default' },
    { "method": 'z_score', "variation": 'default' },
]

date_interval_list = [30, 60, 90, 120]
arg_open_list = [0.2, 0.3, 0.4, 1000]
arg_close_list = [-0.1, 0, 0.1, 1000]
arg_center_list = [0.45, 0.5, 0.55]

# date_interval_list = [30]
# arg_open_list = [0.3, 1000]
# arg_close_list = [0]
# arg_center_list = [0.5]

def signal_args_generator():
    """ Generate combinations of signal arguments for testing. """
    combinations = product(
        method_list,
        date_interval_list,
        arg_center_list,
        arg_open_list,
        arg_close_list,
        arg_open_list,
        arg_close_list,
    )
    for method, date_interval, arg_center,\
            arg_long_open, arg_long_close,\
            arg_short_open, arg_short_close in combinations:
        if arg_long_open >= 1 and arg_short_open >= 1:
            # skip if both long and short open thresholds are off
            continue
        if ((arg_long_open >= 1 and arg_long_close < 1)
            or (arg_long_open < 1 and arg_long_close >= 1)
            or (arg_short_open >= 1 and arg_short_close < 1)
            or (arg_short_open < 1 and arg_short_close >= 1)):
            # skip if long open is off but long close is not
            # skip if short open is off but short close is not
            continue
        arg = SignalArgs(
            method=method['method'],
            variation=method['variation'],
            date_interval=date_interval,
            arg_variation=(f"c{int(arg_center * 100)}"
                    + (f"_lo{int(arg_long_open * 100)}_lc{int(arg_long_close * 100)}"
                        if arg_long_open < 1 else "_loff")
                    + (f"_so{int(arg_short_open * 100)}_sc{int(arg_short_close * 100)}"
                        if arg_short_open < 1 else "_soff")),
            zero_threshold=arg_center,
            long_open_threshold=-arg_long_open,
            long_close_threshold=-arg_long_close,
            short_open_threshold=arg_short_open,
            short_close_threshold=arg_short_close,
        )
        yield arg


def signal_intra_day_upload(spotcode: str, dat: date, args: SignalArgs):
    try:
        df = signal_intra_day(spotcode, dat, args)
        if df is not None and not df.empty:
            upload_trade_with_args(args, df)
        else:
            print(f"No output for {spotcode} on {dat} with args {args}")
    except Exception as e:
        print(f"Error processing {spotcode} on {dat} with args {args}: {e}")


def init_worker():
    global engine
    engine = get_engine()  # Reinitialize the engine in each worker process


def signal_intra_day_all(spotcode: str, bg: date, ed: date):
    date_range = pd.date_range(bg, ed, freq='B')  # Business days only
    signal_args = list(signal_args_generator())

    for dat in tqdm.tqdm(date_range, desc='Processing dates', leave=False):
        if dat.weekday() == 5 or dat.weekday() == 6:
            continue  # Skip weekends
        print(f"Processing {spotcode} on {dat}")
        ratio_df = load_cpr_daily_with_cache(load_dataset_id(spotcode), dat)
        if ratio_df is None or ratio_df.empty:
            print(f"No data for {spotcode} on {dat}")
            continue

        with ProcessPoolExecutor(initializer=init_worker, max_workers=90) as executor:
            futures = [executor.submit(
                signal_intra_day_upload, spotcode, dat, args) for args in signal_args]


if __name__ == '__main__':
    # signal_intra_day_all('510500', date(2025, 7, 1), date(2025, 7, 9))
    signal_intra_day_all('159915', date(2025, 7, 1), date(2025, 7, 9))
    # print(len([x.arg_variation for x in signal_args_generator()]))
    # test_signal_intra_day()
    # fo args in signal_args_generator():
    #     print(f"Processing args: {args}")


