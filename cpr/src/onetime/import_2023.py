import os
import sys
from pathlib import Path
from datetime import datetime, date

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import DATA_DIR
from weekly_update import load_data, clip_data, backtest_data, roll_data

for month in range(1, 12):
    dt_from = date(2022, month, 1)
    if month == 12:
        dt_to = date(2023, 1, 1)
    else:
        dt_to = date(2022, month + 1, 1)
    load_data('159915', dt_from, dt_to)

# clip_data('159915', date(2023, 5, 15), date(2023, 7, 10))

# backtest_data('159915', date(2023, 6, 15), date(2023, 9, 1))
# backtest_data('159915', date(2025, 1, 1), date(2025, 5, 1))

# roll_data('159915', date(2023, 7, 1), date(2023, 12, 31),
#         with_roll_next=True, with_roll_export=False)


