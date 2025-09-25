from pathlib import Path
import sys

sys.path.append((Path(__file__).resolve().parent.parent / 'src').as_posix())

from sakana import SakanaScheduler

sch = SakanaScheduler(
        interval_seconds=10, interval_offset=5,
        timezone_str='Asia/Shanghai',
        work_hours=('16:55', '18:03'),
        work_days={0,1,2,3,4})

sch.set_callback(lambda: print('123'))

sch.run()


