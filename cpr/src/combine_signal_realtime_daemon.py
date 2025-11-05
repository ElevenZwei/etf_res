import click
from sakana import SakanaScheduler
from datetime import datetime, date
from typing import Optional
from combine_signal_realtime import load_and_combine_signals

def task_callback(today: Optional[date] = None):
    if today is None:
        today = date.today()
    load_and_combine_signals(
            today, roll_args_id=1, roll_top=10)


def main():
    scheduler = SakanaScheduler(
            interval_seconds=30,
            interval_offset=6,
            timezone_str='Asia/Shanghai',
            work_hours=('09:30', '15:00'),
            work_days={0, 1, 2, 3, 4}
    )
    scheduler.set_callback(task_callback)
    scheduler.run()


@click.command()
@click.option('-d', '--date', type=click.DateTime(formats=["%Y-%m-%d"]), default=None, help='Run the task for a specific date')
@click.option('-s', '--scheduler', is_flag=True, help='Run the task in scheduler mode')
def click_main(date: Optional[datetime], scheduler: bool):
    if scheduler:
        main()
    else:
        task_callback(today=date.date() if date else None)


if __name__ == '__main__':
    click_main()
    # For testing purposes, run the task directly
    # task_callback(date(2025, 8, 18))

