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

engine = get_engine()

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
    with engine.connect() as conn:
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
    with engine.connect() as conn:
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
    with engine.connect() as conn:
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
    with engine.connect() as conn:
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
    maps to (0, 1) normalization.
    """
    return ((series - series.mean()) / series.std() + 1) / 2

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
    return df

# print(load_ratio_data("159915", time(10, 15), date(2025, 1, 1), date(2025, 1, 31)))
# load_save_clip("159915", time(10, 15), date(2025, 1, 1), date(2025, 1, 31))

def calculate_all_clips(spotcode: str, d1: date, d2: date):
    """
    Calculate all clips for a given spotcode and time interval.
    d2 is inclusive.
    """
    days = [d for d in pd.date_range(d1 - timedelta(days=7), d2) if d.weekday() == 4]  # Only Fridays
    intervals = [timedelta(days=x) for x in [-30, -60, -90, -120]]
    tis = [
            *pd.date_range(start="09:30", end="11:30", freq='1min').time,
            *pd.date_range(start="13:00", end="14:55", freq='1min').time]
    count = 0
    for ti in tis:
        for d in days:
            for interval in intervals:
                count += 1
                bg = (d + interval).date()
                ed = d.date()
                try:
                    df = load_save_clip(spotcode, ti, bg, ed)
                    print(f"#{count} Clip for {spotcode} at {ti} from {bg} to {ed} samples {df.shape[0]} calculated successfully.")
                except Exception as e:
                    print(f"Error calculating clip for {spotcode} at {ti} from {bg} to {ed}: {e}")

if __name__ == "__main__":
    # calculate_all_clips("159915", date(2025, 1, 3), date(2025, 1, 4))
    # calculate_all_clips("510500", date(2025, 1, 3), date(2025, 1, 4))
    # calculate_all_clips("510500", date(2025, 1, 1), date(2025, 7, 9))
    # calculate_all_clips("159915", date(2025, 1, 1), date(2025, 7, 9))
    calculate_all_clips("159915", date(2025, 7, 9), date(2025, 8, 15))

