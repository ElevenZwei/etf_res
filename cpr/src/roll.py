# This file runs the rolling process for the CPR project.

import json
import numpy as np
import pandas as pd
import sqlalchemy as sa
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

from datetime import date, time, timedelta
from dateutil.relativedelta import relativedelta

from config import get_engine, upsert_on_conflict_skip

engine = get_engine()

def load_trade_args(from_id: int, to_id: int) -> pd.DataFrame:
    """
    Load trade arguments for a given range of IDs.
    """
    query = sa.text("""
        select t.id,
            t.date_interval,
            t.variation as trade_variation,
            t.args as trade_args,
            m.id as method_id,
            m.name as method_name,
            m.variation as method_variation,
            m.args as method_args
        from cpr.clip_trade_args t join cpr.method m on t.method_id = m.id
        where t.id >= :from_id and t.id <= :to_id
        order by t.id
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'from_id': from_id, 'to_id': to_id})
    return df.set_index('id')


def parse_trade_args(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # df['trade_args'] = df['trade_args'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    # df['method_args'] = df['method_args'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    # or maybe use NaN
    df['zero_threshold'] = df['trade_args'].apply(
            lambda x: x.get('zero_threshold', 0.5) if isinstance(x, dict) else 0.5)
    df['long_open'] = (df['trade_args'].apply(
            lambda x: x.get('long_open_threshold', 1000) if isinstance(x, dict) else 1000)
            + df['zero_threshold'])
    df['long_close'] = (df['trade_args'].apply(
            lambda x: x.get('long_close_threshold', 1000) if isinstance(x, dict) else 1000)
            + df['zero_threshold'])
    df['short_open'] = (df['trade_args'].apply(
    lambda x: x.get('short_open_threshold', 1000) if isinstance(x, dict) else 1000)
            + df['zero_threshold'])
    df['short_close'] = (df['trade_args'].apply(
            lambda x: x.get('short_close_threshold', 1000) if isinstance(x, dict) else 1000)
            + df['zero_threshold'])
    df['noon_close'] = df['trade_args'].apply(
            lambda x: x.get('noon_close', False) if isinstance(x, dict) else False)
    return df


def load_trade_profits(dataset_id: int, trade_args_ids: List[int],
                       from_dt: date, to_dt: date) -> pd.DataFrame:
    """
    Load trade profits for a given list of trade argument IDs.
    `to_dt` day is inclusive.
    """
    if not trade_args_ids:
        return pd.DataFrame(columns=['trade_args_id', 'profit'])

    # latex: cpr.clip_trade_profit.[dt_open, dt_close] \subset [from_dt, to_dt + 1 day)
    query = sa.text("""
        select dataset_id, trade_args_id,
            dt_open, dt_close,
            price_open, price_close,
            amount, profit,
            profit_percent, profit_logret
        from cpr.clip_trade_profit
        where
            dataset_id = :dataset_id
            and dt_open >= :from_dt and dt_close < :to_dt
            and trade_args_id in :trade_args_ids
        order by trade_args_id, dt_open
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'dataset_id': dataset_id,
            'from_dt': from_dt,
            'to_dt': to_dt + timedelta(days=1),
            'trade_args_ids': tuple(trade_args_ids),
        })
    return df.set_index('trade_args_id')


def trade_profits_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    group by dataset_id and trade_args_id, then aggregate profits
    sum(profit), count(profit), sum(profit_percent), exp(sum(profit_logret)) - 1
    returns a DataFrame with aggregated profits.
    with index set to trade_args_id.
    """
    df = df.copy().reset_index()
    df = df.groupby(['dataset_id', 'trade_args_id']).agg(
            profit=('profit', 'sum'),
            count=('profit', 'count'),
            profit_percent=('profit_percent', 'sum'),
            profit_logret=('profit_logret', 'sum'),
        )
    df['profit_logret'] = np.exp(df['profit_logret']) - 1
    return df.reset_index().set_index('trade_args_id')


def get_roll_method_id(method: str, variation: str, is_static: bool,
                       args: Dict, description: str) -> int:
    query = sa.text("""
        select cpr.get_or_create_roll_method(:method, :variation, :is_static, :args, :description) as id;
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {
            'method': method,
            'variation': variation,
            'is_static': is_static,
            'args': json.dumps(args),
            'description': description
        })
        conn.commit()
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    if df.empty:
        raise ValueError(f"No method found for {method} with variation {variation} and args {args}")
    return (df.iloc[0]['id'])


def get_roll_args_id(roll_method_id: int, dataset_id: int,
                    trade_args_from_id: int, trade_args_to_id: int,
                    pick_count: int) -> int:
    query = sa.text("""
        select cpr.get_or_create_roll_args(
            :dataset_id, :roll_method_id,
            :trade_args_from_id, :trade_args_to_id, :pick_count) as id;
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {
            'roll_method_id': int(roll_method_id),
            'dataset_id': int(dataset_id),
            'trade_args_from_id': int(trade_args_from_id),
            'trade_args_to_id': int(trade_args_to_id),
            'pick_count': int(pick_count),
        })
        conn.commit()
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    if df.empty:
        raise ValueError("No roll args found for the given parameters")
    return int(df.iloc[0]['id'])


def trade_args_filter(df: pd.DataFrame, method_names: Optional[List[str]],
                    long_only: bool, short_only: bool,
                    noon_close: Optional[bool]) -> pd.DataFrame:
    df = df.copy()
    if method_names is not None:
        df = df[df['method_name'].isin(method_names)]
    if noon_close is not None:
        df = df[df['noon_close'] == noon_close]
    if long_only and short_only:
        raise ValueError("Cannot filter for both long and short only.")
    if long_only:
        df = df[df['long_open'] <= 1 and df['long_open'] >= -1]
    if short_only:
        df = df[df['short_open'] <= 1 and df['short_open'] >= -1]
    return df


def roll_method_1_filter(trade_args_df: pd.DataFrame, roll_method_args: Dict[str, any]) -> pd.DataFrame:
    filter_args = roll_method_args.get('filter_args', {})
    method_names = filter_args.get('method_names', None)
    long_only = filter_args.get('long_only', False)
    short_only = filter_args.get('short_only', False)
    noon_close = filter_args.get('noon_close', None)
    return trade_args_filter(trade_args_df, method_names,
                             long_only, short_only, noon_close)


def roll_method_1_sort(profit_df_slice: pd.DataFrame,
                       roll_method_args: Dict[str, any]) -> pd.DataFrame:
    profit_aggr = trade_profits_aggregate(profit_df_slice)
    col = roll_method_args.get('sort_column', None)
    if col is None:
        raise ValueError("Cannot read sort key column")
    profit_aggr['id'] = profit_aggr.index
    profit_aggr = profit_aggr.sort_values(
            by=['profit_logret', 'id'], ascending=[False, True])
    profit_aggr['rank'] = range(1, len(profit_aggr) + 1)
    profit_aggr['weight'] = 1
    profit_aggr['score'] = profit_aggr['profit_logret']
    return profit_aggr[['id', 'rank', 'weight', 'score']]


roll_methods = {
        'best_return': {
            # Static rolling method to find the best return
            # 
            'filter': roll_method_1_filter,
            'sort': roll_method_1_sort,
        },
}


def roll_static_slice(
        profit_df: pd.DataFrame, dt_from: date, dt_to: date,
        validate_days: int, train_days_factor: float,
        ) -> List[Tuple[pd.DataFrame, pd.DataFrame, date, date, date, date]]:
    """
    Make a rolling slice of the profit dataframe.
    `dt_from` and `dt_to` are the start and end dates of the rolling process.
    `dt_to` is inclusive.
    `validate_days` is the number of days for the validation set.
    `train_days_factor` is the factor to multiply `validate_days` to get the training set size.
    Returns a list of tuples, each tuple contains:
        - train_df: DataFrame for training data
        - validate_df: DataFrame for validation data
        - train_from: start date of training data
        - train_to: end date of training data
        - validate_from: start date of validation data
        - validate_to: end date of validation data
    """
    if dt_from is None:
        raise ValueError("No data to roll")
    if validate_days <= 0 or train_days_factor <= 0:
        raise ValueError("validate_days and train_days_factor must be positive")
    if validate_days == 7 or validate_days == 14:
        # align dt_from to the nearest Monday
        dt_from = dt_from - timedelta(days=dt_from.weekday()) + timedelta(weeks=1)
    if validate_days == 30:
        # align dt_from to the first day of next month
        dt_from = dt_from.replace(day=1) + relative_delta(months=1)

    print(f"Rolling from {dt_from} to {dt_to} with validate_days={validate_days}, train_days_factor={train_days_factor}")
    train_days = int(validate_days * train_days_factor)
    slice_count = ((dt_to + timedelta(days=1) - dt_from).days - train_days) // validate_days
    # last slice is train only slice
    slice_count += 1
    slices = []
    # convert dt_from and dt_to to datetime objects with local timezone
    dt_from = pd.Timestamp(dt_from).tz_localize('Asia/Shanghai')
    for i in range(slice_count):
        # 因为是 datetime 对象，所以最后不要减去一天，时间到零点为止，最后一天自然不会包含在内
        train_from = dt_from + timedelta(days=i * validate_days)
        train_to = train_from + timedelta(days=train_days)
        validate_from = train_to
        validate_to = validate_from + timedelta(days=validate_days)
        slices.append((
            profit_df[(profit_df['dt_open'] >= train_from) & (profit_df['dt_close'] < train_to)],
            profit_df[(profit_df['dt_open'] >= validate_from) & (profit_df['dt_close'] < validate_to)],
            train_from, train_to, validate_from, validate_to
        ))
    return slices


@dataclass(frozen=True)
class RollMethodArgs:
    method: str = None
    variation: str = None
    is_static: bool = True
    args: Dict[str, any] = None
    description: str = None


@dataclass(frozen=True)
class RollRunArgs:
    roll_method_args: RollMethodArgs
    dataset_id: int
    # date_from and date_to are the overall date range for the rolling process
    # date_to is inclusive
    date_from: date
    date_to: date
    trade_args_from_id: int
    trade_args_to_id: int
    pick_count: int


def roll_run_static_sort(run_args: RollRunArgs, profit_df: pd.DataFrame) -> pd.DataFrame:
    range_args = run_args.roll_method_args.args.get('range_args', {})
    profit_slice = roll_static_slice(
            profit_df, run_args.date_from, run_args.date_to,
            range_args.get('validate_days', 7),
            range_args.get('train_days_factor', 1),
            )
    roll_sorter = roll_methods[run_args.roll_method_args.method]['sort']
    roll_sorter_args = run_args.roll_method_args.args
    sorted_slices = []
    for idx, tup in enumerate(profit_slice):
        train_df, validate_df, train_from, train_to, validate_from, validate_to = tup
        print(f"Sorting train from {train_from} to {train_to},"
              f" validate from {validate_from} to {validate_to}")
        train_sorted = roll_sorter(train_df, roll_sorter_args)
        # last slice is train only slice, no validate data available
        if idx == len(profit_slice) - 1:
            validate_sorted = train_sorted[0:0].copy()
        else:
            validate_sorted = roll_sorter(validate_df, roll_sorter_args)
        sorted_slices.append((
            train_sorted, validate_sorted,
            train_from, train_to, validate_from, validate_to
        ))
    sorted_export = []
    for slice in sorted_slices:
        sorted_export.append(sort_slice_export(slice, run_args.pick_count))
    roll_rank_dfs, roll_result_dfs = zip(*sorted_export)
    roll_rank_df = pd.concat(roll_rank_dfs, ignore_index=True)
    roll_result_df = pd.concat(roll_result_dfs, ignore_index=True)
    return roll_rank_df, roll_result_df


def sort_slice_export(
        slice: Tuple[pd.DataFrame, pd.DataFrame, date, date, date, date],
        pick_count: int,
        ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transform a slice of data into a format suitable for save to the database.
    Database need two tables:
        roll_rank - for train and validate data
        roll_result - for the final results of the rolling process
    """
    train_sorted, validate_sorted, train_from, train_to, validate_from, validate_to = slice
    print(f"Export train from {train_from} to {train_to}, validate from {validate_from} to {validate_to}")
    print(f"Train sorted: \n{train_sorted.head()}")
    print(f"Validate sorted: \n{validate_sorted.head()}")
    # sorted dataframe has columns: id, rank, weight, score
    # join train_sorted and validate_sorted on id, then rename train columns to predict_xxx.
    roll_rank_df = train_sorted.merge(
            validate_sorted, on='id', suffixes=('_train', '_validate'))
    roll_rank_df = roll_rank_df.rename(columns={
        'id': 'trade_args_id',
        'rank_train': 'predict_rank',
        'weight_train': 'predict_weight',
        'score_train': 'predict_score',
        'rank_validate': 'real_rank',
        'weight_validate': 'real_weight',
        'score_validate': 'real_score',
    })
    # filter roll_rank_df to keep only the columns we need
    roll_rank_df = roll_rank_df[[
        'trade_args_id', 'predict_rank', 'predict_weight', 'predict_score',
        'real_rank', 'real_weight', 'real_score'
    ]]
    # filter roll_rank_df to keep only the rows with predict_rank <= keep_count or real_rank <= keep_count
    roll_rank_df = roll_rank_df[
            (roll_rank_df['predict_rank'] <= pick_count)
            | (roll_rank_df['real_rank'] <= pick_count)]
    roll_rank_df['train_dt_from'] = train_from
    roll_rank_df['train_dt_to'] = train_to
    roll_rank_df['validate_dt_from'] = validate_from
    roll_rank_df['validate_dt_to'] = validate_to
 
    # roll_result_df is the output of applying train predict to validate data.
    roll_result_df = train_sorted[['id', 'weight', 'rank']]
    roll_result_df = roll_result_df.rename(columns={
        'id': 'trade_args_id',
        'weight': 'predict_weight',
        'rank': 'predict_rank',
    })
    roll_result_df = roll_result_df[
            roll_result_df['predict_rank'] <= pick_count]
    roll_result_df['dt_from'] = validate_from
    roll_result_df['dt_to'] = validate_to

    return roll_rank_df, roll_result_df


def get_roll_args_id_from_run_args(run_args: RollRunArgs) -> int:
    # save roll method args to the database
    method_id = get_roll_method_id(
            run_args.roll_method_args.method,
            run_args.roll_method_args.variation,
            run_args.roll_method_args.is_static,
            run_args.roll_method_args.args,
            run_args.roll_method_args.description
    )
    roll_args_id = get_roll_args_id(
            method_id, run_args.dataset_id,
            run_args.trade_args_from_id, run_args.trade_args_to_id,
            run_args.pick_count)
    return roll_args_id


def save_roll_output(run_args: RollRunArgs, rank_df: pd.DataFrame, result_df: pd.DataFrame) -> pd.DataFrame:
    """ Save the rolling output to the database.
    Transform run_args to roll_args_id, then save the rank_df and result_df to the database.
    Return the result_df with roll_args_id added.
    """
    roll_args_id = get_roll_args_id_from_run_args(run_args)

    print("uploading roll_rank to database.")
    rank_df['roll_args_id'] = roll_args_id
    rank_df.to_sql('roll_rank', engine, schema='cpr',
            if_exists='append', index=False,
            method=upsert_on_conflict_skip,
            chunksize=1000)

    print("uploading roll_result to database.")
    result_df['roll_args_id'] = roll_args_id
    result_df.to_sql('roll_result', engine, schema='cpr',
            if_exists='append', index=False,
            method=upsert_on_conflict_skip,
            chunksize=1000)
    return result_df


def roll_run(run_args: RollRunArgs):
    trade_args_df = load_trade_args(run_args.trade_args_from_id, run_args.trade_args_to_id)
    trade_args_df = parse_trade_args(trade_args_df)
    roll_filter = roll_methods[run_args.roll_method_args.method]['filter']
    if roll_filter is None:
        raise ValueError(f"Filter for method {run_args.roll_method_args.method} not found")
    filtered_trade_args_df = roll_filter(trade_args_df, run_args.roll_method_args.args)
    print(f"Filtered trade args: {len(filtered_trade_args_df)} rows")

    profit_df = load_trade_profits(
            run_args.dataset_id, filtered_trade_args_df.index.tolist(),
            run_args.date_from, run_args.date_to)
    if profit_df.empty:
        raise ValueError("No profits found for the given trade arguments")

    if run_args.roll_method_args.is_static:
        roll_rank_df, roll_result_df = roll_run_static_sort(run_args, profit_df)
        return save_roll_output(run_args, roll_rank_df, roll_result_df)
    else:
        raise NotImplementedError("Dynamic rolling is not implemented yet")


# dt_from = date(2025, 1, 1)
# dt_to = date(2025, 3, 31)
# df = load_trade_profits(4, [11813], dt_from, dt_to)
# print(df)
# slices = roll_static_slice(df, dt_from, dt_to, 7, 2)
# print(f"Found {len(slices)} slices")
# print(slices)
#
# df = load_trade_args(11813, 11815)
# df = parse_trade_args(df)
# print(df)
# print(df[['trade_args', 'method_args', 'zero_threshold', 'long_open', 'long_close', 'short_open', 'short_close']])
# print(df['trade_args'].apply(lambda x: json.loads(x) if isinstance(x, str) else x).to_dict())
 
