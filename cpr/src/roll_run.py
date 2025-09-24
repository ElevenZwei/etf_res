# 这个是运行 roll.py 里面滚动选取策略逻辑的运行器。

from datetime import datetime, date, timedelta

from roll import RollMethodArgs, RollRunArgs, roll_run, get_roll_args_id_from_run_args

best_return1 = RollMethodArgs(
    method="best_return",
    variation="logret_t1w_v1w",
    is_static=True,
    args={
        "range_args": {
            "validate_days": 7,
            "train_days_factor": 1,
        },
        "filter_args": {
            "noon_close": False,
        },
        'sort_column': 'profit_logret',
    },
    description="Best return with log returns, previous week to predict this week",
)


def gen_roll_args_list(dataset_id: int, dt_bg: date, dt_ed: date) -> list[RollRunArgs]:
# 这里的 date_from 和 date_to 是滚动选取的时间范围，
# date_from 要写的比回测的开始时间早一些，因为她需要包含训练的时间范围。
    dt_from = (dt_bg - timedelta(days=60))
    dt_to = dt_ed
    roll_run_args_list = [
        RollRunArgs(
            roll_method_args=best_return1,
            dataset_id=dataset_id,
            date_from=dt_from,
            date_to=dt_to,
            trade_args_from_id=1,
            trade_args_to_id=8092,
            pick_count=5000,
        ),
    ]
    return roll_run_args_list


def get_roll_args_ids(dataset_id: int, dt_bg: date, dt_ed: date) -> set[int]:
    args_list = gen_roll_args_list(dataset_id, dt_bg, dt_ed)
    roll_args_ids = { get_roll_args_id_from_run_args(roll_run_args)
                     for roll_run_args in args_list }
    return roll_args_ids


def main(dataset_id: int, dt_bg: date, dt_ed: date) -> set[int]:
    args_list = gen_roll_args_list(dataset_id, dt_bg, dt_ed)
    roll_args_ids = set()
    for roll_run_args in args_list:
        df = roll_run(roll_run_args)
        roll_args_ids.add(int(df['roll_args_id'].iloc[0]))
    return roll_args_ids


if __name__ == "__main__":
    main(
        dataset_id=3,
        dt_bg=date(2025, 8, 10),
        dt_ed=date(2025, 8, 25),
    )


