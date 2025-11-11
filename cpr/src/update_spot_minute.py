import datetime
import pandas as pd
from dl_oi_new import dl_spot_data
from config import DATA_DIR


spot_dl = dl_spot_data('159915', datetime.date(2025, 10, 28), datetime.datetime.now().date())
spot_dl['dt'] = pd.to_datetime(spot_dl['dt'])
spot_dl = spot_dl.set_index('dt').tz_convert('Asia/Shanghai')
print(spot_dl)

spot_old = pd.read_csv(DATA_DIR / 'fact' / 'spot_159915_2025_dsp.csv')
spot_old['dt'] = pd.to_datetime(spot_old['dt'])
spot_old = spot_old.set_index('dt')

spot_new = pd.concat([spot_old, spot_dl])
print(spot_new)
spot_new.to_csv(DATA_DIR / 'fact' / 'spot_minute_159915.csv')

