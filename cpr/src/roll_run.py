from datetime import datetime, date

from roll import RollMethodArgs, RollRunArgs, roll_run

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

roll_run_args_list = [
    RollRunArgs(
        roll_method_args=best_return1,
        dataset_id=3,
        date_from=date(2025, 1, 1),
        date_to=date(2025, 1, 31),
        trade_args_from_id=1,
        trade_args_to_id=8092,
        pick_count=5000,
    ),
]

for roll_run_args in roll_run_args_list:
    roll_run(roll_run_args)

