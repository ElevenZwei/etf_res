import time
import traceback
import pytz
from datetime import datetime, timedelta

class SakanaScheduler:
    """
    这个是 AI 生成的类似于 crontab 的定时器类型
    """
    def __init__(self,
                 interval_seconds: int = 60,
                 interval_offset: int = 0,
                 timezone_str: str = 'Asia/Shanghai',
                 work_hours: tuple = ('09:30', '15:00'),
                 work_days: set = {0, 1, 2, 3, 4}):

        self.interval = timedelta(seconds=interval_seconds)
        self.tz = pytz.timezone(timezone_str)
        self.start_time = self._parse_time(work_hours[0])
        self.end_time = self._parse_time(work_hours[1])
        self.work_days = work_days
        self.cb = lambda: print('empty job')
        # add offset to self.start_time
        self.start_time = (
                datetime.combine(datetime.now(self.tz).date(), self.start_time)
                + timedelta(seconds=interval_offset)).time()

    def set_callback(self, cb):
        self.cb = cb

    def _parse_time(self, time_str: str) -> datetime.time:
        return datetime.strptime(time_str, '%H:%M').time()

    def _is_working_time(self, dt: datetime) -> bool:
        return (dt.weekday() in self.work_days
                and self.start_time <= dt.time() < self.end_time)

    def _next_execution(self) -> datetime:
        # time in self.tz
        now = datetime.now(self.tz)

        # Case 1: 今日が営業日かつ開始前
        if (now.weekday() in self.work_days
            and now.time() < self.start_time):
            return self.tz.localize(
                    datetime.combine(now.date(), self.start_time)
            )

        # Case 2: 営業時間中
        if self._is_working_time(now):
            next_exec = self._next_interval(now)
            print("next_exec", next_exec)
            if next_exec.time() < self.end_time:
                return next_exec
            else:
                return self._next_workday_start(now)

        # Case 3: 営業時間外
        return self._next_workday_start(now)

    def _next_workday_start(self, dt: datetime) -> datetime:
        next_day = dt + timedelta(days=1)
        while next_day.weekday() not in self.work_days:
            next_day += timedelta(days=1)
        return self.tz.localize(
                datetime.combine(next_day.date(), self.start_time)
        )

    def _next_interval(self, now: datetime) -> datetime:
        intervals_since_start = (now - self._today_start(now)) // self.interval
        return self._today_start(now) + (intervals_since_start + 1) * self.interval

    def _today_start(self, dt: datetime) -> datetime:
        return self.tz.localize(
                datetime.combine(dt.date(), self.start_time)
        )

    def run(self):
        while True:
            next_run = self._next_execution()
            wait_seconds = (next_run - datetime.now(self.tz)).total_seconds()

            if wait_seconds > 0:
                print(f"[SAKANA] Next run at: {next_run:%Y-%m-%d %H:%M:%S}")
                time.sleep(wait_seconds)

            if self._is_working_time(datetime.now(self.tz)):
                try:
                    print(f"[ZAP] Executed at: {datetime.now(self.tz):%H:%M:%S}")
                    # Main logic here
                    self.cb()
                except Exception as e:
                    print(f"[BOOM] Error: {e}")
                    print(traceback.format_exc())

