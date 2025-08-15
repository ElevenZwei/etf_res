# 这个文件的作用是导出所有需要使用的参数，保存成一个json 文件。
# 导出参数之后运行交易可以逻辑上独立于数据库，单独在其他环境运行。
#
# 这个文件需要输入导出的 roll_args_id 和选择的参数组合数量 top count 。
# 还需要输入应用参数的开始和结束日期 dt_from 和 dt_to 。
# 然后它会读取 cpr.roll_args join cpr.roll_method 表格，导出 method 信息。
# 再读取 cpr.roll_result 表，找到 roll_args_id 相同，
# 并且 dt_from 和 dt_to 被包含的条目，
# 这些条目里面有需要使用的 trade_args_id 列表。
# 先整理出上面的信息，
# 然后读取 cpr.trade_args 表，把 trade_args_id 翻译成对应的交易参数。
# 再读取 cpr.clip 表格，把交易参数转换成具体的触发数值。
# 这个过程好长。

import click
import json
import pytz
import numpy as np
import pandas as pd
import sqlalchemy as sa
from typing import Dict, List, Tuple, Optional, TypeAlias, Union
from dataclasses import dataclass

from datetime import date, time, datetime, timedelta
from dateutil.relativedelta import relativedelta

from config import get_engine, NpEncoder

engine = get_engine()

def load_roll_args_info(roll_args_id: int) -> Dict[str, Union[int, str, Dict[str, Union[int, str]]]]:
    """Load roll arguments and method information from the database by roll_args_id."""
    with engine.connect() as conn:
        query = sa.text("""
            select
                ra.id as roll_args_id,
                ra.trade_args_from_id,
                ra.trade_args_to_id,
                ds.id as dataset_id,
                ds.spotcode as dataset_spotcode,
                rm.id as roll_method_id, 
                rm.name as roll_method_name,
                rm.variation as roll_method_variation,
                rm.is_static,
                rm.args as roll_method_args
            from cpr.roll_args ra
            join cpr.roll_method rm on ra.roll_method_id = rm.id
            join cpr.dataset ds on ra.dataset_id = ds.id
            where ra.id = :roll_args_id;
        """)
        df = pd.read_sql(query, conn, params={"roll_args_id": roll_args_id})
    
    if df.empty:
        raise ValueError(f"No roll_args found for roll_args_id: {roll_args_id}")
    if len(df) > 1:
        raise ValueError(f"Multiple roll_args found for roll_args_id: {roll_args_id}")
    
    row = df.iloc[0]
    return {
        "spotcode": row['dataset_spotcode'],
        "dataset_id": row['dataset_id'].astype(int),
        "roll_args_id": row['roll_args_id'].astype(int),
        "roll_method_id": row['roll_method_id'].astype(int),
        "roll_method_name": row['roll_method_name'],
        "roll_method_variation": row['roll_method_variation'],
        "roll_method_json": row['roll_method_args'],
        "roll_trade_args_from_id": row['trade_args_from_id'].astype(int),
        "roll_trade_args_to_id": row['trade_args_to_id'].astype(int),
    }


def load_roll_result(roll_args_id: int, top: int, dt_from: datetime, dt_to: datetime) -> pd.DataFrame:
    """
    Load roll results from the database by roll_args_id, limited to top entries within date range.
    returns a DataFrame with roll_args_id, trade_args_id, dt_from, dt_to, rank, weight.
    """
    with engine.connect() as conn:
        query = sa.text("""
            select
                roll_args_id, trade_args_id,
                dt_from as roll_dt_from,
                dt_to as roll_dt_to,
                predict_rank as rank,
                predict_weight as weight
            from cpr.roll_result
            where roll_args_id = :roll_args_id
            and predict_rank <= :top
            and dt_from <= :dt_from
            and dt_to >= :dt_to
        """)
        df = pd.read_sql(query, conn, params={
            "roll_args_id": roll_args_id,
            "top": top,
            "dt_from": dt_from,
            "dt_to": dt_to,
        })
    if df.empty:
        raise ValueError(
                f"No roll results found for roll_args_id: {roll_args_id}, top: {top}, "
                f"date range: {dt_from} to {dt_to}")
    return df


def roll_result_to_dict(df: pd.DataFrame) -> Dict[str, Union[int, date, Dict[int, float]]]:
    """
    Convert roll result DataFrame to a list of dictionaries.
    keys are:
        - roll_args_id: int
        - roll_dt_from: date
        - roll_dt_to: date
        - trade_args: Dict[int, float]
    """
    weight_sum = df['weight'].sum()
    return {
        "roll_args_id": df['roll_args_id'].iloc[0].astype(int),
        "roll_dt_from": (df['roll_dt_from'].iloc[0]
                .tz_convert('Asia/Shanghai').to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')),
        "roll_dt_to": (df['roll_dt_to'].iloc[0]
                .tz_convert('Asia/Shanghai').to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')),
        "trade_args": dict(zip(
            df['trade_args_id'].astype(int),
            df['weight'].astype(float) / (weight_sum if weight_sum > 0 else 1.0),
        ))
    }


def load_trade_args(trade_args_id: int) -> Dict[str, Union[int, Dict[str, Union[int, str]]]]:
    with engine.connect() as conn:
        query = sa.text("""
            select
                id as trade_args_id,
                method_id as trade_args_method_id,
                date_interval,
                args as trade_args_json
            from cpr.clip_trade_args
            where id = :trade_args_id;
        """)
        df = pd.read_sql(query, conn, params={"trade_args_id": trade_args_id})
    if df.empty:
        raise ValueError(f"No trade_args found for trade_args_id: {trade_args_id}")
    return {
        "trade_args_id": df['trade_args_id'].iloc[0].astype(int),
        "trade_args_method_id": df['trade_args_method_id'].iloc[0].astype(int),
        "trade_args_date_interval": df['date_interval'].iloc[0].astype(int),
        "trade_args_json": (df['trade_args_json'].iloc[0]),
    }


def trade_args_parse_threshold(trade_args_json: any) -> Dict[str, Optional[float]]:
    if isinstance(trade_args_json, str):
        obj = json.loads(trade_args_json)
    else:
        obj = trade_args_json
    zt = obj['zero_threshold']
    lo = obj['long_open_threshold'] + zt
    lc = obj['long_close_threshold'] + zt
    so = obj['short_open_threshold'] + zt
    sc = obj['short_close_threshold'] + zt
    if abs(lo) > 10:
        lo = None
        lc = None
    if abs(so) > 10:
        so = None
        sc = None
    return {
        "long_open_threshold": lo,
        "long_close_threshold": lc,
        "short_open_threshold": so,
        "short_close_threshold": sc,
    }


def date_range_of_trade_args(today: date, date_interval: int) -> Tuple[date, date]:
    ed = today - timedelta(days=today.weekday() + 3)  # last Friday
    bg = ed - relativedelta(days=int(date_interval))
    return bg, ed


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


def load_clip(ds_id: int, trade_args_method_id: int, ti: time, d1: date, d2: date):
    """
    Load clip data from the database for a given dataset and method within a date range.
    returns the data column as a dict.
    format: { "ratio": List[float], "ratio_diff": List[float] }
    """
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
            'method_id': str(trade_args_method_id),
            'range_id': str(range_id),
        })
    if df.empty:
        raise ValueError(f"No clip data found for dataset {ds_id}, method {trade_args_method_id}, time {ti}, dates {d1} to {d2}")
    data = df['data'].iloc[0]
    return data


CLIP_CACHE: Dict[str, pd.DataFrame] = {}
def load_clip_with_cache(
    ds_id: int, trade_args_method_id: int, ti: time, d1: date, d2: date,
    ) -> pd.DataFrame:
    """
    Load clip data with caching.
    """
    key = f"{ds_id}-{trade_args_method_id}-{ti}-{d1}-{d2}"
    if key in CLIP_CACHE:
        return CLIP_CACHE[key]
    clip_data = load_clip(ds_id, trade_args_method_id, ti, d1, d2)
    CLIP_CACHE[key] = clip_data
    return clip_data


def iterate_minute(bg: time, ed: time, step: timedelta = timedelta(minutes=1)) -> List[time]:
    res = []
    current = bg
    while current <= ed:
        res.append(current)
        current = (datetime.combine(date.today(), current) + step).time()
    return res


def load_clips_for_trade_args(
        dataset_id: int, trade_args_info: any, today: date
        ) -> Dict[time, List[float]]:
    d1, d2 = date_range_of_trade_args(today, trade_args_info['trade_args_date_interval'])
    time_intervals = [
            *iterate_minute(time(9, 35), time(11, 28)),
            *iterate_minute(time(13, 0), time(14, 54)),
    ]
    res = {}
    for ti in time_intervals:
        clip_data = load_clip_with_cache(
            dataset_id, trade_args_info['trade_args_method_id'], ti, d1, d2)
        res[ti] = clip_data["ratio_diff"]
    return res


def cut_clips_for_trade_args(
        clips: Dict[time, List[float]], trade_args_info: any,
        ) -> List[Dict[str, Union[str, float]]]:
    """
    Cut clips to the threshold defined by trade_args_info.
    """
    res = []
    thresholds = trade_args_parse_threshold(trade_args_info['trade_args_json'])
    threshold_keys = list(thresholds.keys())
    index_array = np.arange(0, 10, 0.05)
    for ti, clip_data in clips.items():
        entry = {}
        for key in threshold_keys:
            value = thresholds[key]
            if value is not None:
                index = np.searchsorted(index_array, value)
                entry[key] = clip_data[index] if index < len(clip_data) else None
            else:
                entry[key] = None
        # rename entry keys to match the threshold keys
        entry = {
            key.replace('_threshold', ''): entry[key] for key in threshold_keys
        }
        entry['time'] = ti.strftime('%H:%M:%S')
        res.append(entry)
    return res


def load_roll_export_info() -> Dict[str, Union[int, str]]:
    """Load roll export information."""
    return {
        "export_version": 1,
        "export_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "export_description": "Roll export for trading parameters.",
    }


def is_in_same_week(dt1: date, dt2: date) -> bool:
    """
    Check if two dates are in the same week.
    """
    return dt1.isocalendar()[1] == dt2.isocalendar()[1] and dt1.year == dt2.year


def roll_export(roll_args_id: int, top: int, dt_from: date, dt_to: date):
    if not is_in_same_week(dt_from, dt_to):
        raise ValueError("dt_from and dt_to must be in the same week.")
    info = load_roll_export_info()
    info.update(load_roll_args_info(roll_args_id))
    tz = pytz.timezone('Asia/Shanghai')
    dt_from_datetime = tz.localize(datetime.combine(dt_from, time(0, 0, 0)))
    dt_to_datetime = tz.localize(datetime.combine(dt_to, time(23, 59, 59)))
    df = load_roll_result(roll_args_id, top, dt_from_datetime, dt_to_datetime)
    info.update(roll_result_to_dict(df))
    info['input_dt_from'] = dt_from_datetime.strftime('%Y-%m-%d %H:%M:%S')
    info['input_dt_to'] = dt_to_datetime.strftime('%Y-%m-%d %H:%M:%S')
    info['roll_top'] = top

    trade_args_list = list(info['trade_args'].keys())
    trade_args_details = [
        load_trade_args(trade_args_id) for trade_args_id in trade_args_list
    ]
    for trade_args in trade_args_details:
        trade_args['trade_args_thresholds'] = trade_args_parse_threshold(trade_args['trade_args_json'])
        clips = load_clips_for_trade_args(info['dataset_id'], trade_args, dt_from)
        trade_args['trigger'] = cut_clips_for_trade_args(clips, trade_args)
    info['trade_args_details'] = trade_args_details

    return info


@click.command()
@click.option('-r', '--roll_args_id', type=int, required=True, help='Roll arguments ID to export.')
@click.option('-t', '--top', type=int, required=True, help='Top count of parameters to export.')
@click.option('-b', '--dt_from', type=str, required=True, help='Start date (YYYY-MM-DD).')
@click.option('-e', '--dt_to', type=str, required=True, help='End date (YYYY-MM-DD).')
def click_main(roll_args_id: int, top: int, dt_from: str, dt_to: str):
    """
    Command line interface for exporting roll parameters.
    """
    dt_from_date = datetime.strptime(dt_from, '%Y-%m-%d').date()
    dt_to_date = datetime.strptime(dt_to, '%Y-%m-%d').date()
    result = roll_export(roll_args_id, top, dt_from_date, dt_to_date)
    print(json.dumps(result, indent=2, cls=NpEncoder))


if __name__ == "__main__":
    click_main()

