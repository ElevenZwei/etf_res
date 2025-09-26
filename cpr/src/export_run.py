"""
这个文件的目标是根据导出的 roll_test.json 参数文件，还有输入一段时间的历史数据，然后它可以得出每分钟的交易信号。
输入历史数据的方式需要做成两种模块，一种是读入 CSV 然后选择我们需要的时间范围，另一种是从数据库中读取。
输出格式暂定成 DataFrame，然后可以直接存储到数据库中。
"""

import click
import json
import numpy as np
import pandas as pd
import sqlalchemy as sa
from typing import Dict, List, Tuple, Optional, TypeAlias, Union, Any
from dataclasses import dataclass

from datetime import date, time, datetime, timedelta
from dateutil.relativedelta import relativedelta

from config import DATA_DIR, get_engine, upsert_on_conflict_skip
from dl_oi import dl_calc_oi_range

engine = get_engine()

@dataclass()
class RollExport:
    roll_args_id: int
    roll_top: int
    spotcode: str
    # 测试交易的时间范围
    # input_dt_from and input_dt_to are inclusive.
    # input_dt_to should be like '2025-08-17 23:59:59' to include the whole day.
    input_dt_from: datetime
    input_dt_to: datetime
    # trade_time_from and trade_time_to are inclusive.
    trade_time_from: time
    trade_time_to: time
    trade_weight: Dict[int, float]
    # 交易参数的触发条件，trade_args_id -> trigger DataFrame
    # trigger DataFrame 的格式是：
    #  - time: HH:MM:SS
    #  - long_open: float
    #  - long_close: float
    #  - short_open: float
    #  - short_close: float
    trade_trigger: Dict[int, pd.DataFrame]
    roll_export_id: Optional[int] = None


def read_roll_export_file(file_path: str) -> RollExport:
    """从 JSON 文件加载 roll 参数。"""
    with open(file_path, 'r', encoding='utf-8') as f:
        json_str = f.read()
    return parse_roll_export(json_str)


def read_roll_export_db(roll_args_id: int, top: int,
                      dt_from: date, dt_to: date) -> RollExport:
    """
    从数据库加载 roll 参数。
    input dt_from and dt_to are inclusive.
    """
    # latex: [input_dt_from, input_dt_to] \subset [sql_dt_from, sql_dt_to)
    query = sa.text("""
        select id, args from cpr.roll_export
        where roll_args_id = :roll_args_id
            and top = :top
            and dt_from <= :dt_from
            and dt_to > :dt_to
        limit 1;
        """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'roll_args_id': roll_args_id,
            'top': top,
            'dt_from': dt_from,
            'dt_to': dt_to,
        })
    if df.empty:
        raise ValueError(f"No roll export entry found for id {roll_args_id}, top {top}, "
                         f"dt_from {dt_from}, dt_to {dt_to}.")
    res = parse_roll_export(df['args'].iloc[0])
    res.roll_export_id = df['id'].iloc[0]
    print(f"Loaded roll export from DB with id {res.roll_export_id}")
    return res


def parse_roll_export(json_str_obj: Union[str, Any]) -> RollExport:
    if isinstance(json_str_obj, str):
        obj = json.loads(json_str_obj)
    else:
        obj = json_str_obj
    trade_weight = {int(k): v for k, v in obj['trade_args'].items()}
    trade_trigger = {}
    for item in obj['trade_args_details']:
        trade_args_id = int(item['trade_args_id'])
        trigger_df = pd.DataFrame(item['trigger'])
        trade_trigger[trade_args_id] = trigger_df
    return RollExport(
            roll_args_id=int(obj['roll_args_id']),
            roll_top=int(obj['roll_top']),
            spotcode=obj['spotcode'],
            input_dt_from=datetime.strptime(
                obj['input_dt_from'], '%Y-%m-%d %H:%M:%S'),
            input_dt_to=datetime.strptime(
                obj['input_dt_to'], '%Y-%m-%d %H:%M:%S'),
            trade_time_from=(datetime.strptime(
                obj['trade_time_from'], '%H:%M:%S').time()
                if 'trade_time_from' in obj else time(9, 30)),
            trade_time_to=(datetime.strptime(
                obj['trade_time_to'], '%H:%M:%S').time()
                if 'trade_time_to' in obj else time(15, 0)),
            trade_weight=trade_weight,
            trade_trigger=trade_trigger,
    )


def merge_trade_trigger(trade_weight: Dict[int, float],
                        trade_trigger: Dict[int, pd.DataFrame]) -> pd.DataFrame:
    """
    合并所有交易参数的触发条件。
    输入参数：
        trade_weight: Dict[int, float] - 交易参数的权重，key 是 trade_args_id，value 是权重。
        trade_trigger: Dict[int, pd.DataFrame] - 交易参数的触发条件，key 是 trade_args_id，value 是触发条件的 DataFrame。
    输出内容：
        返回一个 DataFrame，包含所有交易参数的触发条件，按时间 time 和 trade_args_id 索引。
    数据列包括：
        - time: 交易时间，格式为 HH:MM:SS
        - trade_args_id: 交易参数 ID
        - weight: 交易参数的权重
        - long_open: 多头开仓阈值
        - long_close: 多头平仓阈值
        - short_open: 空头开仓阈值
        - short_close: 空头平仓阈值
    """

    df_frags = []
    for trade_args_id, weight in trade_weight.items():
        if trade_args_id in trade_trigger:
            df = trade_trigger[trade_args_id].copy()
            df['trade_args_id'] = trade_args_id
            df['weight'] = weight
            if df.empty:
                print(f"Warning: trade_args_id {trade_args_id} has no triggers.")
                continue
            # remove columns that are all NaN or empty, required by pandas concat.
            df_cleaned = df.dropna(how='all', axis=1)
            df_frags.append(df_cleaned)
    if not df_frags:
        raise ValueError("No trade triggers found.")
    merged_df = pd.concat(df_frags, ignore_index=True)
    if 'long_open' not in merged_df.columns:
        merged_df['long_open'] = np.nan
    if 'long_close' not in merged_df.columns:
        merged_df['long_close'] = np.nan
    if 'short_open' not in merged_df.columns:
        merged_df['short_open'] = np.nan
    if 'short_close' not in merged_df.columns:
        merged_df['short_close'] = np.nan

    merged_df['time'] = pd.to_datetime(merged_df['time'], format='%H:%M:%S').dt.time
    merged_df.set_index(['time', 'trade_args_id'], inplace=True, drop=False)
    merged_df.sort_index(inplace=True)
    # print(f"Merged trade trigger DataFrame:\n{merged_df.head(20)}")
    return merged_df


def cut_trade_trigger(df: pd.DataFrame, roll: RollExport) -> pd.DataFrame:
    time_from = roll.trade_time_from
    time_to = roll.trade_time_to
    # latex: df.time \in [trade_time_from, trade_time_to]
    df = df[(df['time'] >= time_from) & (df['time'] <= time_to)].copy()
    return df


def read_oi_csv(csv_path: str, dt_from: date, dt_to: date) -> pd.DataFrame:
    """
    Read OI data from a CSV file and filter by date range.
    Args:
        csv_path (str): Path to the CSV file containing OI data.
        dt_from (date): Start date for filtering.
        dt_to (date): End date for filtering, inclusive.
    """
    # localize the date range to pandas timestamp with timezone
    dt_from = pd.Timestamp(dt_from).tz_localize('Asia/Shanghai')
    dt_to = pd.Timestamp(dt_to).tz_localize('Asia/Shanghai')
    df = pd.read_csv(csv_path, parse_dates=['dt'])
    df = df[(df['dt'] >= dt_from) & (df['dt'] < dt_to + timedelta(days=1))]
    return df


def read_oi_db(spotcode: str, dt_from: date, dt_to: date) -> pd.DataFrame:
    """
    Read OI data from the database for a specific spotcode and date range.
    Args:
        spotcode (str): The spot code for which to read OI data.
        dt_from (date): Start date for filtering.
        dt_to (date): End date for filtering, inclusive.
    """
    df = dl_calc_oi_range(spotcode, dt_from, dt_to)
    df['dt'] = pd.to_datetime(df['dt'], utc=True)
    df['dt'] = df['dt'].dt.tz_convert('Asia/Shanghai')
    return df


def downsample_time(df: pd.DataFrame, interval_sec: int):
    df = df.resample(f'{interval_sec}s').first()
    # 这里跳过没有开盘的时间
    df = df.loc[~df.isna().all(axis=1)]
    return df


def convert_oi_df(df: pd.DataFrame) -> pd.DataFrame:
    # print(f"Converting OI DataFrame with shape {df.shape} and columns {df.columns.tolist()}")
    # print(df.head())
    df = df[['dt', 'call_oi_sum', 'put_oi_sum']]
    df = df.rename(columns={'call_oi_sum': 'call', 'put_oi_sum': 'put'})
    df['dt_raw'] = df['dt']
    df.set_index('dt', inplace=True, drop=True)
    df.sort_index(inplace=True)
    df = downsample_time(df, 60)  # downsample to 1 minute

    # calculate cpr, cpr_open, cpr_diff from call and put
    # 这里是 pyright 无法判断的一个属性，因为 df.index 是 DatetimeIndex 这个信息从类型系统中丢失了
    df['dt_date'] = df.index.date
    df['dt_time'] = df.index.time
    df.reset_index(inplace=True, drop=False)
    # call_open column is first_value(oi) over (partition by dt_date order by dt)
    df['call_open'] = df.groupby('dt_date')['call'].transform(lambda x: x.iloc[0])
    # put_open column is first_value(oi) over (partition by dt_date order by dt)
    df['put_open'] = df.groupby('dt_date')['put'].transform(lambda x: x.iloc[0])
    df['cpr'] = (df['call'] - df['put']) / (df['call'] + df['put'])
    df['cpr_open'] = (df['call_open'] - df['put_open']) / (df['call_open'] + df['put_open'])
    df['cpr_diff'] = df['cpr'] - df['cpr_open']
    # print(f"Converted OI DataFrame:\n{df}")
    return df


def join_oi_trigger(oi_df: pd.DataFrame, 
                    trigger_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join OI DataFrame with trade trigger DataFrame.
    The trigger_df should have 'time' and 'trade_args_id' as index.
    """
    oi_df = oi_df.reset_index(drop=True)
    oi_df = oi_df[['dt', 'dt_raw', 'dt_date', 'dt_time', 'cpr_diff']]
    oi_df['time'] = oi_df['dt_time'].astype('category')
    oi_df.set_index(['time'], inplace=True, drop=True)
    trigger_df = trigger_df.reset_index(drop=True)
    trigger_df['time'] = trigger_df['time'].astype('category')
    trigger_df.set_index(['time'], inplace=True, drop=True)
    merged_df = oi_df.join(trigger_df, how='left', lsuffix='_oi', rsuffix='_trigger')
    merged_df.reset_index(inplace=True, drop=True)
    merged_df.sort_values(by=['dt'], inplace=True)
    print(f"Joined OI and Trigger DataFrame:\n{merged_df.head(10)}\n{merged_df.tail(10)}")
    return merged_df


def split_trade_args(merged_df: pd.DataFrame) -> Dict[float, pd.DataFrame]:
    """
    Split the merged DataFrame into a dictionary of DataFrames by trade_args_id.
    """
    merged_df.reset_index(drop=True, inplace=True)
    merged_df['trade_args_id'] = merged_df['trade_args_id'].astype('category')
    merged_df.set_index(['trade_args_id'], inplace=True, drop=False)
    # non-trading time rows
    # loc + [np.nan] to get DataFrame and keep the index as np.nan
    if np.nan in merged_df.index:
        nan_rows: pd.DataFrame = merged_df.loc[[np.nan]].copy()
    else:
        nan_rows = merged_df.iloc[0:0].copy()

    trade_args_dict = { np.nan: nan_rows }
    for trade_args_id in merged_df['trade_args_id'].unique():
        if trade_args_id is None or pd.isna(trade_args_id):
            continue
        trade_df: pd.DataFrame = merged_df.loc[[trade_args_id]].copy()
        # add non-trading time rows into split dataframes
        trade_df = pd.concat([trade_df, nan_rows], ignore_index=True)
        trade_df['weight'] = trade_df['weight'].astype('float64')
        # sort by time
        trade_df.reset_index(drop=True, inplace=True)
        trade_df.sort_values(by='dt', inplace=True)
        trade_args_dict[trade_args_id] = trade_df
    return trade_args_dict


def split_trade_cycle(trade_df: pd.DataFrame) -> List[pd.DataFrame]:
    """Split the trade argument DataFrame into a list of DataFrames by date."""
    trade_df.reset_index(drop=True, inplace=True)
    # remove rows with dt_time > 11:25 and < 12:00
    # this is to skip trading the time between 11:25 and 12:00
    # same with cpr_diff_sig.py
    trade_df = trade_df[
            ~((trade_df['dt_time'] > time(11, 25))
              & (trade_df['dt_time'] < time(12, 0)))
            ].copy()
    trade_df['dt_date'] = trade_df['dt_date'].astype('category')
    trade_df.set_index(['dt_date'], inplace=True, drop=False)
    result = []
    for dt_date in trade_df['dt_date'].unique():
        trade_cycle_df = trade_df.loc[dt_date].copy()
        if isinstance(trade_cycle_df, pd.Series):
            trade_cycle_df = trade_cycle_df.to_frame().T
        result.append(trade_cycle_df)
    return result


def gen_trade_zone(cycle_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate trade zones based on the cycle DataFrame.
    Logic:
        if cpr_diff <= long_open:
            zone = 'long_open'
        elif cpr_diff <= long_close:
            zone = 'long_hold'
        elif cpr_diff >= short_open:
            zone = 'short_open'
        elif cpr_diff >= short_close:
            zone = 'short_hold'
        else:
            zone = 'close'
    """
    cycle_df['zone'] = cycle_df.apply(
            lambda row: 'long_open' if row['cpr_diff'] <= row['long_open'] else
                    ('long_hold' if row['cpr_diff'] <= row['long_close'] else
                     ('short_open' if row['cpr_diff'] >= row['short_open'] else
                      ('short_hold' if row['cpr_diff'] >= row['short_close'] else 'close'))),
            axis=1)
    return cycle_df


def gen_trade_position(zone_df: pd.DataFrame) -> pd.DataFrame:
    """Generate trade positions based on the zone DataFrame."""
    last_position = 0.0
    positions = []
    for tup in zone_df.itertuples():
        zone = tup.zone
        if last_position == 0.0:
            if zone in ['long_open']:
                last_position = 1.0
            elif zone in ['short_open']:
                last_position = -1.0
        elif last_position == 1.0:
            if zone in ['long_hold', 'long_open']:
                last_position = 1.0
            elif zone in ['short_open']:
                last_position = -1.0
            else:
                last_position = 0.0
        elif last_position == -1.0:
            if zone in ['short_hold', 'short_open']:
                last_position = -1.0
            elif zone in ['long_open']:
                last_position = 1.0
            else:
                last_position = 0.0
        positions.append(last_position)
    zone_df['position'] = positions
    zone_df['weighted_position'] = zone_df['position'] * zone_df['weight']
    return zone_df


def aggr_trade_position(position_df_list: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Aggregate trade positions from a list of DataFrames.
    Each DataFrame should have 'dt', 'position', and 'weight' columns.
    """
    if not position_df_list:
        # raise ValueError("No position DataFrames provided.")
        return pd.DataFrame(columns=[
            'dt', 'dt_raw', 'open_count', 'position', 'weight_sum'])

    aggr_df = pd.concat(position_df_list, ignore_index=True)
    aggr_df['is_open'] = aggr_df['position'].abs()
    aggr_df = aggr_df.groupby(['dt']).agg({
        'dt_raw': 'first',
        'is_open': 'sum',
        'weighted_position': 'sum',
        'weight': 'sum',
    }).reset_index()
    aggr_df = aggr_df.rename(columns={
        'is_open': 'open_count',
        'weighted_position': 'position',
        'weight': 'weight_sum',
    })
    # filter invalid weight sum
    aggr_df['weight_sum'] = aggr_df['weight_sum'].astype('float64')
    invalid_weight_mask = (
              (~np.isclose(aggr_df['weight_sum'], 1.0))
            & (~np.isclose(aggr_df['weight_sum'], 0.0))
            )
    invalid_weight_count = invalid_weight_mask.sum()
    if invalid_weight_count > 0:
        print(f"Warning: {invalid_weight_count} rows have invalid weight sum.")
        print(f"Invalid rows:\n{aggr_df[invalid_weight_mask]}")
        aggr_df = aggr_df[~invalid_weight_mask]
    aggr_df = aggr_df[['dt', 'dt_raw', 'open_count', 'position', 'weight_sum']]
    return aggr_df


def aggr_set_meta(aggr_df: pd.DataFrame, roll_export: RollExport) -> pd.DataFrame:
    aggr_df['roll_args_id'] = roll_export.roll_args_id
    aggr_df['top'] = roll_export.roll_top
    return aggr_df


def aggr_cut(aggr_df: pd.DataFrame) -> pd.DataFrame:
    aggr_df.reset_index(drop=True, inplace=True)
    aggr_df = aggr_df[['roll_args_id', 'top', 'dt', 'dt_raw', 'open_count', 'position']]
    return aggr_df


def aggr_filter_diff(aggr_df: pd.DataFrame) -> pd.DataFrame:
    # keep only rows with position different from previous row
    aggr_df['position_diff'] = aggr_df['position'].diff().fillna(0.0)
    aggr_df = aggr_df[aggr_df['position_diff'] != 0.0].copy()
    aggr_df.drop(columns=['position_diff'], inplace=True)
    return aggr_df


def run_roll_export(roll_export: RollExport, oi_df: pd.DataFrame) -> pd.DataFrame:
    trigger_df = merge_trade_trigger(
            roll_export.trade_weight,
            roll_export.trade_trigger)
    trigger_df = cut_trade_trigger(trigger_df, roll_export)
    converted_df = convert_oi_df(oi_df)
    merged_df = join_oi_trigger(converted_df, trigger_df)
    split_trade_args_dict = split_trade_args(merged_df)
    position_dfs = []
    for trade_args_id, trade_df in split_trade_args_dict.items():
        trade_cycles = split_trade_cycle(trade_df)
        trade_cycles = [gen_trade_position(gen_trade_zone(cycle_df)) for cycle_df in trade_cycles]
        position_dfs.extend(trade_cycles)

    aggr_df = aggr_trade_position(position_dfs)
    aggr_df = aggr_set_meta(aggr_df, roll_export)
    aggr_df = aggr_cut(aggr_df)
    return aggr_df


@dataclass(frozen=True)
class RollExportFrom:
    """这是一个命令行输入的参数类型，指定读取 roll export json 的方法和运行参数"""
    source: str = 'file' # 'file' or 'db' or 'json'
    file_path: Optional[str] = None
    db_roll_args_id: Optional[int] = None
    db_roll_top: Optional[int] = None
    db_dt_from: Optional[date] = None
    db_dt_to: Optional[date] = None
    json_str: Optional[str] = None


def read_roll_export(roll_export_from: RollExportFrom) -> RollExport:
    """
    Read roll export parameters from a specified source.
    roll_export_from: RollExportFrom - 包含读取参数的来源信息。
    """
    if roll_export_from.source == 'file':
        return read_roll_export_file(roll_export_from.file_path)
    if roll_export_from.source == 'db':
        return read_roll_export_db(
                roll_export_from.db_roll_args_id,
                roll_export_from.db_roll_top,
                roll_export_from.db_dt_from,
                roll_export_from.db_dt_to)
    if roll_export_from.source == 'json':
        return parse_roll_export(roll_export_from.json_str)
    raise ValueError(f"Unknown roll export source: {roll_export_from.source}")


def save_roll_export_run(aggr_df: pd.DataFrame, roll_export: RollExport) -> int:
    aggr_df = aggr_df[['dt', 'dt_raw', 'position']].copy()
    aggr_df['roll_export_id'] = roll_export.roll_export_id
    with engine.connect() as conn:
        aggr_df.to_sql('roll_export_run', conn,
                schema='cpr', if_exists='append', index=False,
                method=upsert_on_conflict_skip, chunksize=1000)


def run_roll_export_from(roll_export_from: RollExportFrom,
                         oi_from: str,
                         dt_from: date, dt_to: date) -> pd.DataFrame:
    """
    从指定的 JSON 文件和 OI CSV 文件中运行 roll 参数。
    dt_from 和 dt_to 是可选的，如果没有提供，则使用 roll_export.input_dt_from 和 roll_export.input_dt_to。
    dt_to 是包含在内的，即到这一天完全结束。
    """
    roll_export = read_roll_export(roll_export_from)
    input_from = roll_export.input_dt_from.date()
    # input_dt_to is like '2025-08-17 23:59:59', so input_to is inclusive.
    input_to = roll_export.input_dt_to.date()

    if dt_from is None:
        dt_from = input_from
    if dt_to is None:
        dt_to = input_to
    if dt_from < input_from:
        raise ValueError(f"dt_from {dt_from} is earlier than roll_export.input_dt_from {roll_export.input_dt_from}.")
    if dt_to > input_to:
        raise ValueError(f"dt_to {dt_to} is later than roll_export.input_dt_to {roll_export.input_dt_to}.")
    print(f"Running roll args from {roll_export_from} and OI from {oi_from}, dt_from: {dt_from}, dt_to: {dt_to}")
    if oi_from == 'db':
        oi_df = read_oi_db(roll_export.spotcode, dt_from, dt_to)
    else:
        oi_df = read_oi_csv(oi_from, dt_from, dt_to)
    df = run_roll_export(roll_export, oi_df)
    if roll_export.roll_export_id is not None:
        save_roll_export_run(df, roll_export)
    return df


@click.command()
@click.option('-j', '--roll_export_from', type=str, required=True,
            help='Path to the roll arguments JSON file or integer <db_roll_args_id> to read from database.')
@click.option('-i', '--oi_from', type=str, required=True,
            help='Path to the OI CSV file or "db" to read from database.')
@click.option('-b', '--dt_from', type=str, default=None,
            help='Start date for the OI data in YYYY-MM-DD format. Defaults to roll_export.input_dt_from.')
@click.option('-e', '--dt_to', type=str, default=None,
            help='End date for the OI data in YYYY-MM-DD format. Defaults to roll_export.input_dt_to.')
def click_main(roll_export_from: str, oi_from: str, dt_from: str, dt_to: str):
    """
    Command line interface to run the roll arguments processing.
    """
    dt_from_date = datetime.strptime(dt_from, '%Y-%m-%d').date() if dt_from else None
    dt_to_date = datetime.strptime(dt_to, '%Y-%m-%d').date() if dt_to else None
    aggr_df = run_roll_export_from(
            RollExportFrom(
                source='file' if roll_export_from.endswith('.json') else 'db',
                file_path=roll_export_from if roll_export_from.endswith('.json') else None,
                db_roll_args_id=int(roll_export_from) if roll_export_from.isdigit() else None,
                db_roll_top=10,
                db_dt_from=dt_from_date,
                db_dt_to=dt_to_date,
            ), oi_from, dt_from_date, dt_to_date)
    diff_df = aggr_filter_diff(aggr_df)
    print(f"Final DataFrame after filtering:\n{diff_df.head(20)}")


if __name__ == "__main__":
    click_main()


