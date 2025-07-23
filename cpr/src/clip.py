# This file reads cpr.daily table and create normalized clips
# at a given time and in a given date interval.
# And store the clip into cpr.clip table.

from datetime import time, date, datetime, timedelta
from typing import Callable, Dict, List

import json
import numpy as np
import pandas as pd
import sqlalchemy as sa
from config import get_engine


def load_ratio_data(spotcode: str, ti: time, d1: date, d2: date)-> pd.DataFrame:
    """
    Load ratio data for a given time and spotcode.
    """
    query = sa.text("""
        with ds as (
            select id from cpr.dataset
            where spotcode = :spotcode
            and expiry_priority = 1
            and strike = 0
            limit 1
        ), daily as (
            select dt, ratio, ratio_diff, dataset_id
            from cpr.daily cross join ds
            where dataset_id = ds.id
            and ti = :ti
            and dt >= :d1 and dt <= :d2
        )
        select dt, ratio, ratio_diff, spotcode, dataset_id
        from daily join cpr.dataset d on d.id = daily.dataset_id
    """)
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn, params={
            'spotcode': spotcode,
            'ti': ti,
            'd1': d1,
            'd2': d2
        })
    if df.empty:
        return None
    return df.set_index('dt').sort_index()


def load_range_id(ti: time, d1: date, d2: date) -> int:
    query = sa.text("""
        select cpr.get_or_create_dt_range(:d1, :d2, :t1, :t1) as id;
    """)
    with get_engine().connect() as conn:
        result = conn.execute(query, {
            'd1': d1,
            'd2': d2,
            't1': ti
        })
        conn.commit()  # Or it will rollback
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    if df.empty:
        raise ValueError(f"No range found for time {ti} and dates {d1} to {d2}")
    return df.iloc[0]['id']


def load_method_id(method: str, variation: str, args) -> int:
    query = sa.text("""
        select cpr.get_or_create_method(:method, :variation, :args, null) as id;
    """)
    with get_engine().connect() as conn:
        result = conn.execute(query, {
            'method': method,
            'variation': variation,
            'args': json.dumps(args) if args else None
        })
        conn.commit()  # Or it will rollback
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    if df.empty:
        raise ValueError(f"No method found for {method} with variation {variation} and args {args}")
    return df.iloc[0]['id']

def make_ratio_clip(df: pd.DataFrame, method: Callable[[pd.Series], pd.Series]) -> Dict[str, List[float]]:
    return {
        'ratio': normalize_series(df['ratio'], method, 0.05).tolist(),
        'ratio_diff': normalize_series(df['ratio_diff'], method, 0.05).tolist(),
    }

def save_clip_to_db(clip: Dict[str, List[float]], dataset_id: int, range_id: int, method_id: int):
    query = sa.text("""
        select cpr.get_or_create_clip(:dataset_id, :range_id, :method_id, :data) as id;
    """)
    datastr = json.dumps(clip)
    with get_engine().connect() as conn:
        result = conn.execute(query, {
            'dataset_id': str(dataset_id),
            'range_id': str(range_id),
            'method_id': str(method_id),
            'data': datastr, # Assuming clip can be serialized to string
        })
        conn.commit()  # Or it will rollback
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    if df.empty:
        raise ValueError("Failed to save clip to database")
    return df.iloc[0]['id']

def normalize_series(series: pd.Series, method: Callable[[pd.Series], pd.Series], step: float) -> np.ndarray:
    # filter series to remove NaN values
    series = series.dropna()
    s2 = method(series)
    # find out what does 0, 0.05, ... in s2 correspond to in the original series
    grid = np.arange(0, 1 + step, step)
    df = pd.DataFrame({ "raw": series, "norm": s2 })
    df = df.sort_values(by='norm')
    interpolated = np.interp(grid, df['norm'], df['raw'])
    return interpolated

def norm_min_max(series: pd.Series, p: float = 0) -> pd.Series:
    """
    Normalize a pandas Series using percentile normalization.
    p ranges from 0 to 1, where p=0 corresponds to the minimum value.
    """
    return (series - series.quantile(p)) / (series.quantile(1 - p) - series.quantile(p))

def norm_percentile(series: pd.Series) -> pd.Series:
    """
    Normalize a pandas Series using percentile normalization.
    The result is a series of percentiles.
    """
    return series.rank(pct=True)

def norm_z_score(series: pd.Series) -> pd.Series:
    """
    Normalize a pandas Series using z-score normalization.
    """
    return (series - series.mean()) / series.std()

norm_methods = {
    'min_max': {
        'default': {
            'arg': { 'p': 0.0 },
            'func': norm_min_max,
        },
        'p_05': {
            'arg': { 'p': 0.05 },
            'func': lambda s: norm_min_max(s, 0.05),
        },
        'p_10': {
            'arg': { 'p': 0.10 },
            'func': lambda s: norm_min_max(s, 0.10),
        },
    },
    'percentile': {
        'default': { 'arg': None, 'func': norm_percentile, },
    },
    'z_score': {
        'default': { 'arg': None, 'func': norm_z_score, },
    }
}

def load_save_clip(spotcode: str, ti: time, d1: date, d2: date):
    df = load_ratio_data(spotcode, ti, d1, d2)
    range_id = load_range_id(ti, d1, d2)
    for method_name, variations in norm_methods.items():
        for variation_name, variation in variations.items():
            method_id = load_method_id(method_name, variation_name, variation['arg'])
            clip = make_ratio_clip(df, variation['func'])
            save_clip_to_db(clip, df['dataset_id'].iloc[0], range_id, method_id)

# print(load_ratio_data("159915", time(10, 15), date(2025, 1, 1), date(2025, 1, 31)))
load_save_clip("159915", time(10, 15), date(2025, 1, 1), date(2025, 1, 31))
