# 这个文件的作用是读取 cpr.roll_result 。
# 根据表格里面各组交易参数的权重，
# 以及每组参数在 cpr.clip_trade_backtest 表格的仓位历史记录，
# 总结出合并之后的仓位历史记录。
# 储存到 cpr.roll_merge 表格中。
# 
# 这里的仓位合成是指将不同交易参数的仓位历史记录按照权重进行加权平均，
# 以得到一个综合的仓位历史记录。
# 这个过程可以帮助我们更好地理解不同交易参数的浮动表现。

# 计算的范围有几种指定方法，需要指定数据集的范围和时间范围。
# 时间范围需要手工输入，数据范围有几种方案指定。
# 例如使用 roll_method_name + dataset_id 来指定，
# 或者使用 roll_args_id 来指定。
# 一个 roll_method_name 可以对应多个 roll_method_id,
# 一个 roll_method_id 可以对应多个 roll_args_id。
# roll_args_id 已经包含了 dataset_id ，但是没有时间范围。

import json
import numpy as np
import pandas as pd
import sqlalchemy as sa
from typing import Dict, List, Tuple, Optional, TypeAlias, Union
from dataclasses import dataclass

from datetime import date, time, timedelta
from dateutil.relativedelta import relativedelta
import sqlalchemy.dialects.postgresql as pg

from config import get_engine

engine = get_engine()

def upsert_table_roll_merged(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = pg.insert(table.table).values(data)
    update_dict = {'position': stmt.excluded.position} 
    stmt = stmt.on_conflict_do_update(
            index_elements=['roll_args_id', 'top', 'dt'],
            set_=update_dict)
    conn.execute(stmt)


def load_roll_args(roll_args_id: int) -> Dict[str, int]:
    """Load roll arguments from the database by roll_args_id."""
    with engine.connect() as conn:
        query = sa.text("""
                SELECT * FROM cpr.roll_args
                WHERE id = :roll_args_id""")
        df = pd.read_sql(query, conn, params={"roll_args_id": roll_args_id})
    if df.empty:
        raise ValueError(f"No roll_args found for roll_args_id: {roll_args_id}")
    if len(df) > 1:
        raise ValueError(f"Multiple roll_args found for roll_args_id: {roll_args_id}")
    row = df.iloc[0]
    roll_args = {
            "roll_args_id": row['id'],
            "roll_method_id": row['roll_method_id'],
            "dataset_id": row['dataset_id'],
            "trade_args_from_id": row['trade_args_from_id'],
            "trade_args_to_id": row['trade_args_to_id'],
    }
    return roll_args


def load_roll_result(roll_args_id: int, top: int, dt_from: date, dt_to: date) -> pd.DataFrame:
    """
    load roll result for a specific roll arguments ID and date range.
    return columns:
        roll_args_id, trade_args_id, dt_from, dt_to, rank, weight
    """
    with engine.connect() as conn:
        # input dt_from and dt_to are inclusive
        # sql dt_from is inclusive, dt_to is exclusive
        # find all roll results that overlap with input [dt_from, dt_to]
        query = sa.text("""
            SELECT
                roll_args_id, trade_args_id, dt_from, dt_to,
                predict_rank as rank, predict_weight as weight
            FROM cpr.roll_result
            WHERE roll_args_id = :roll_args_id
            AND predict_rank <= :top
            AND dt_to > :dt_from
            AND dt_from <= :dt_to
        """)
        df = pd.read_sql(query, conn, params={
            "roll_args_id": roll_args_id,
            "top": top,
            "dt_from": dt_from,
            "dt_to": dt_to,
        })
    if df.empty:
        raise ValueError(f"No roll result found for roll_args_id: {roll_args_id}, top: {top}, from: {dt_from}, to: {dt_to}")
    return df


def load_trade_history(dataset_id: int, trade_args_id: int, dt_from: date, dt_to: date) -> pd.DataFrame:
    """
    Load trade history for a specific dataset and trade arguments ID.
    input dt_from and dt_to are inclusive
    """
    print(f"Loading trade history for dataset_id={dataset_id}, trade_args_id={trade_args_id}, "
        f"from {dt_from} to {dt_to}")
    with engine.connect() as conn:
        # latex: cpr.clip_trade_backtest.dt \in [dt_from, dt_to]
        query = sa.text("""SELECT * FROM cpr.clip_trade_backtest
                    WHERE dataset_id = :dataset_id
                    AND trade_args_id = :trade_args_id
                    AND dt >= :dt_from
                    AND dt <= :dt_to""")
        df = pd.read_sql(query, conn, params={
            "dataset_id": int(dataset_id),
            "trade_args_id": int(trade_args_id),
            "dt_from": dt_from,
            "dt_to": dt_to,
        })
    return df


RangeType: TypeAlias = Tuple[date, date]
WeightsType: TypeAlias = Dict[int, float]
RangeWeightsType: TypeAlias = Tuple[RangeType, WeightsType]


def roll_result_ranges(roll_result: pd.DataFrame) -> List[RangeType]:
    """Get the date ranges from the roll result."""
    if roll_result.empty:
        return []
    roll_result = roll_result.sort_values(by='dt_from')
    df = roll_result[['dt_from', 'dt_to']].drop_duplicates()
    return list(zip(df['dt_from'].tolist(), df['dt_to'].tolist()))


def roll_result_weights(roll_result: pd.DataFrame, range_arg: RangeType) -> WeightsType:
    """Calculate weights for each roll method based on the roll result."""
    range_from, range_to = range_arg
    roll_result = roll_result[
            (roll_result['dt_from'] >= range_from)
            & (roll_result['dt_to'] <= range_to)
    ]
    weights = {}
    weight_sum = roll_result['weight'].sum()
    for _, row in roll_result.iterrows():
        args_id = row['trade_args_id']
        weight = row['weight']
        if args_id in weights:
            weights[args_id] += weight
        else:
            weights[args_id] = weight
    # Normalize weights
    if weight_sum > 0:
        for args_id in weights:
            weights[args_id] /= weight_sum
    return weights


def roll_result_range_weights(roll_result: pd.DataFrame) -> List[RangeWeightsType]:
    """Get the weights for each date range in the roll result."""
    ranges = roll_result_ranges(roll_result)
    weights = []
    for range_arg in ranges:
        weights_for_range = roll_result_weights(roll_result, range_arg)
        weights.append((range_arg, weights_for_range))
        print(f"Range {range_arg} weights: {weights_for_range}")
    return weights


def merge_range_weights(dataset_id: int, range_weights: RangeWeightsType) -> pd.DataFrame:
    """Merge the weights for a specific date range into a DataFrame."""
    range_arg, weights = range_weights
    trade_args_ids = list(weights.keys())
    if not trade_args_ids:
        raise ValueError(f"No trade arguments IDs found in weights for range: {range_arg}")
    # Load trade history for the specified dataset and trade arguments IDs
    df_list = [ load_trade_history(
            dataset_id, trade_args_id, range_arg[0], range_arg[1])
            for trade_args_id in trade_args_ids ]
    # multiply each DataFrame by its corresponding weight
    for df in df_list:
        df['weight'] = 0.0
        if df.empty:
            continue
        trade_args_id = df['trade_args_id'].iloc[0]
        weight = weights.get(trade_args_id, 0)
        if weight > 0:
            df['weight'] = weight
            df['position'] *= weight
        else:
            df['position'] = 0.0
    # Remove empty DataFrames
    # df_list can't be empty here.
    df_1 = df_list[0]
    df_list = [df for df in df_list if not df.empty]
    if not df_list:
        return df_1
    # Concatenate all DataFrames into one
    df = pd.concat(df_list, ignore_index=True)
    # print(df.head())
    # Group by date and sum the positions
    df = df.groupby(['dt', 'dataset_id']).agg({
        'position': 'sum',
        'weight': 'sum',
    }).reset_index()
    return df


def calculate_merged_positions(roll_args_id: int, top: int, dt_from: date, dt_to: date) -> pd.DataFrame:
    """
    Calculate merged positions based on roll arguments ID and date range.
    dt_from and dt_to are inclusive.
    """
    roll_args = load_roll_args(roll_args_id)
    dataset_id = roll_args['dataset_id']
    # Load roll result
    roll_result = load_roll_result(roll_args_id, top, dt_from, dt_to)
    # Get range weights
    range_weights = roll_result_range_weights(roll_result)
    if not range_weights:
        raise ValueError(f"No range weights found for roll_args_id: {roll_args_id}, top: {top}, from: {dt_from}, to: {dt_to}")
    # Merge positions for each range
    merged_positions_list = [
        merge_range_weights(dataset_id, rw) for rw in range_weights
    ]
    # Concatenate all merged positions into one DataFrame
    merged_positions = pd.concat(merged_positions_list, ignore_index=True)
    print(f"Merged positions for roll_args_id {roll_args_id} from {dt_from} to {dt_to}:")
    print(merged_positions.head())
    print(merged_positions.tail())

    # only keep the lines with position value different from previous line
    merged_positions = merged_positions.sort_values(by="dt")
    merged_positions['position_diff'] = merged_positions['position'].diff().fillna(0)
    merged_positions = merged_positions[merged_positions['position_diff'] != 0]

    merged_positions = merged_positions[['dt', 'position',]]
    merged_positions['roll_args_id'] = roll_args_id
    merged_positions['top'] = top
    print(f"Merged positions change for roll_args_id {roll_args_id} from {dt_from} to {dt_to}:")
    print(merged_positions)

    return merged_positions


def save_merged_positions(merged_positions: pd.DataFrame) -> None:
    """Save the merged positions to the database."""
    with engine.connect() as conn:
        merged_positions.to_sql(
            'roll_merged',
            conn,
            schema='cpr',
            if_exists='append',
            index=False,
            method=upsert_table_roll_merged,
            chunksize=1000,
        )
    print("Merged positions saved to roll_merged table.")


if __name__ == "__main__":
    # for roll_args_id in [1]:
    for roll_args_id in [1, 2]:
        merged_positions = calculate_merged_positions(
                roll_args_id=roll_args_id,
                top=10,
                dt_from=date(2025, 1, 1),
                dt_to=date(2025, 5, 18))
        save_merged_positions(merged_positions)


