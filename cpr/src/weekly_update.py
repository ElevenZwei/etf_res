# 这个文件的作用是完成自动化的每周数据更新和配置更新的任务。

from dl_oi import dl_calc_oi_range, oi_csv_merge
from csv2cpr import upload_oi_df_to_cpr
from tick2bar import convert_df_to_bars
from clip import calculate_all_clips
from cpr_diff_sig import signal_intra_day_all
from roll_run import main as roll_main, get_roll_args_ids as get_roll_args_ids
from roll_merge import save_merged_positions, calculate_merged_positions
from roll_export import roll_export, roll_export_save_db

import click
import pandas as pd
import sqlalchemy as sa
from config import get_engine
from datetime import date, datetime, timedelta
from typing import List, Tuple

engine = get_engine()

def get_dataset_id(spot: str):
    if spot == '159915':
        return 3
    elif spot == '510500':
        return 4
    else:
        raise ValueError(f"Unknown spot code: {spot}")


def load_data(spot: str, dt_bg: date, dt_ed: date):
    dl_calc_oi_range(spot, dt_bg, dt_ed)
    oi_df = oi_csv_merge(spot)
    upload_oi_df_to_cpr(spot, oi_df)
    convert_df_to_bars(spot, oi_df)
    with engine.connect() as conn:
        query = sa.text("""
            select cpr.update_daily(:dt_bg, :dt_ed, :dataset_id)
        """)
        conn.execute(query, {
            "dt_bg": dt_bg,
            "dt_ed": dt_ed,
            "dataset_id": get_dataset_id(spot)})
        conn.commit()
        query = sa.text("""
            select * from (
                (select * from "cpr"."daily"
                where dataset_id = :dataset_id
                and dt >= :dt_bg and dt <= :dt_ed
                order by dt asc 
                limit 5)
                union all
                (select * from "cpr"."daily"
                where dataset_id = :dataset_id
                and dt >= :dt_bg and dt <= :dt_ed
                order by dt desc
                limit 5)
            ) as sub order by dt asc
        """)
        df = pd.read_sql(query, conn, params={
            "dt_bg": dt_bg,
            "dt_ed": dt_ed,
            "dataset_id": get_dataset_id(spot)})
        print("Updated daily data head and tail:")
        print(df)


def clip_data(spot: str, dt_bg: date, dt_ed: date):
    calculate_all_clips(spot, dt_bg, dt_ed)


def make_week_clip(dt_bg: date, dt_ed: date) -> List[Tuple[date, date]]:
    # week_bg is Monday after or equal dt_bg
    week_bg = dt_bg - timedelta(days=dt_bg.weekday())
    if week_bg < dt_bg:
        week_bg = week_bg + timedelta(days=7)
    # week_ed is Monday before or equal dt_ed
    week_ed = dt_ed - timedelta(days=dt_ed.weekday())
    if dt_ed.weekday() >= 4:  # if dt_ed is Friday or later, include that week
        week_ed = week_ed + timedelta(days=7)
    print(f"Calculating full weeks from {week_bg} to {week_ed}")
    if week_bg >= week_ed:
        print("No full week in the given date range, skipping backtest trade args update.")
        return []
    # add 1 to include the next week after week_ed
    weeks = (week_ed - week_bg).days // 7 + 1
    result = []
    for i in range(weeks):
        wbg = week_bg + timedelta(days=i*7)
        wed = wbg + timedelta(days=6)
        result.append((wbg, wed))
    print(f"Full weeks: {result}")
    return result


def backtest_data(spot: str, dt_bg: date, dt_ed: date):
    signal_intra_day_all(spot, dt_bg, dt_ed)
    with engine.connect() as conn:
        print("Updating intraday spot profit records...")
        query = sa.text("""
            select cpr.update_intraday_spot_clip_profit_range(:dataset_id, 1, 8082, :dt_bg, :dt_ed)
        """)
        conn.execute(query, {
            "dt_bg": dt_bg,
            "dt_ed": dt_ed,
            "dataset_id": get_dataset_id(spot)})
        conn.commit()
        weeks = make_week_clip(dt_bg, dt_ed)[:-1]  # exclude the last incomplete week
        query = sa.text("""
            select * from cpr.get_best_clip_trade_args(:week_bg, :week_ed, :dataset_id, cnt => 5)
        """)
        df = pd.read_sql(query, conn, params={
            "week_bg": weeks[-1][0] if weeks else dt_bg,
            "week_ed": weeks[-1][1] if weeks else dt_ed,
            "dataset_id": get_dataset_id(spot)})
        print("Best clip trade args for recent weeks:")
        print(df)


def roll_data(spot: str, dt_bg: date, dt_ed: date, with_roll_next: bool = True):
    dataset_id = get_dataset_id(spot)
    weeks = make_week_clip(dt_bg, dt_ed)
    week_bg = weeks[0][0] if weeks else dt_bg
    week_ed = weeks[-1][1] if weeks else dt_ed
    if with_roll_next:
        # 这里的参数会传给 gen_roll_args_list，它会把起点向前移动 60 天
        # 因为滚动选取需要包含训练的时间范围
        # 然后再传给 roll_static_slice 函数，这个函数选取的时间切片会包含最后一个不完整的星期。
        # 所以这里的 end 参数至少要是想要预测的那个星期的星期一。
        roll_args_idset = roll_main(dataset_id, week_bg, week_ed)
    else:
        roll_args_idset = get_roll_args_ids(dataset_id, week_bg, week_ed)

    for roll_args_id in roll_args_idset:
        top = 10
        merged_positions = calculate_merged_positions(
                roll_args_id=roll_args_id,
                top=top,
                dt_from=dt_bg,
                dt_to=dt_ed)
        save_merged_positions(merged_positions)

    if with_roll_next:
        for roll_args_id in roll_args_idset:
            for week_bg, week_ed in weeks:
                exp = roll_export(roll_args_id, top, week_bg, week_ed)
                roll_export_id = roll_export_save_db(exp)
                print(f"Saved roll export id {roll_export_id} for roll_args_id {roll_args_id} from {week_bg} to {week_ed}")


def weekly_update(spot: str, dt_bg: date, dt_ed: date,
                  with_roll: bool = True,
                  with_roll_next: bool = True):
    # weeks = make_week_clip(dt_bg, dt_ed)
    load_data(spot, dt_bg, dt_ed)
    clip_data(spot, dt_bg, dt_ed)
    backtest_data(spot, dt_bg, dt_ed)
    if with_roll:
        roll_data(spot, dt_bg, dt_ed, with_roll_next=with_roll_next)



@click.command()
@click.option('-s', '--spot', type=click.Choice(['159915', '510500']), required=True, help='Spot code to update')
@click.option('-b', '--date-bg', type=click.DateTime(formats=["%Y-%m-%d"]), required=False, help='Start date (YYYY-MM-DD)', default=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"))
@click.option('-e', '--date-ed', type=click.DateTime(formats=["%Y-%m-%d"]), required=False, help='End date (YYYY-MM-DD)', default=datetime.now().strftime("%Y-%m-%d"))
@click.option('--no-roll', is_flag=True, default=False, help='Skip roll update')
@click.option('--no-roll-next', is_flag=True, default=False, help='Skip roll next week prediction update')
def click_main(spot: str, date_bg: datetime, date_ed: datetime,
               no_roll: bool, no_roll_next: bool):
    if date_ed is None:
        date_ed = datetime.now()
    if date_bg is None:
        date_bg = date_ed - timedelta(days=14)
    dt_bg = date_bg.date()
    dt_ed = date_ed.date()
    if dt_bg > dt_ed:
        raise ValueError("Start date must be before or equal to end date")
    print(f"Starting weekly update for spot {spot} from {dt_bg} to {dt_ed}")
    weekly_update(spot, dt_bg, dt_ed,
                  with_roll=not no_roll,
                  with_roll_next=not no_roll_next)
    print("Weekly update completed.")


if __name__ == "__main__":
    click_main()

