import glob
import pandas as pd
from tqdm import tqdm
from backtest.config import DATA_DIR
from backtest.scripts.black_scholes import calculate_d1, calculate_delta, calculate_impv

tqdm.pandas(desc="Processing")

DATE_SUFFIX = '0401_0709'
OPT_INPUT_FILE = f'{DATA_DIR}/input/opt_159915_2025_{DATE_SUFFIX}.csv'
OPT_DSP_OUTPUT_FILE = f'{DATA_DIR}/input/opt_159915_2025_{DATE_SUFFIX}_dsp.csv'
SPOT_INPUT_FILE = f'{DATA_DIR}/input/spot_159915_2025.csv'
SPOT_DSP_OUTPUT_FILE = f'{DATA_DIR}/input/spot_159915_2025_dsp.csv'
OPT_GREEKS_OUTPUT_FILE = f'{DATA_DIR}/input/opt_159915_2025_{DATE_SUFFIX}_greeks.csv'
OPT_GREEKS_CONCAT_OUTPUT_FILE = f'{DATA_DIR}/input/opt_159915_2025_greeks.csv'


def downsample_time(df: pd.DataFrame, interval_sec: int):
    df = df.resample(f'{interval_sec}s').first().dropna()
    return df

def line_greeks(option_price: float, future_price: float,
                strike: float, expiry_date: pd.Timestamp, 
               current_date: pd.Timestamp, cp: int) -> float:
    """计算期权的 delta"""
    # set expiry_date with timezone
    if expiry_date.tzinfo is None:
        expiry_date = expiry_date.tz_localize('Asia/Shanghai')
    if current_date >= expiry_date:
        return 0.0
    t = (expiry_date - current_date).days / 365.0  # 转换为年
    if t <= 0:
        return 0.0
    rate = 0.02
    volatility = calculate_impv(option_price, future_price, strike, rate, t, cp)
    if (volatility is None) or (volatility <= 0):
        return 0.0
    d1 = calculate_d1(future_price, strike, rate, t, volatility)
    delta = calculate_delta(future_price, strike, rate, t, volatility, cp, d1)
    return delta

def df_delta(df: pd.DataFrame):
    # 逐行计算 delta
    # 每一行的输入是 'dt', 'expirydate', 'strike', 'spot_price', 'cp'
    df['delta'] = df.progress_apply(
        lambda row: line_greeks(
            option_price=row['closep'],
            future_price=row['spot_price'],
            strike=row['strike'],
            expiry_date=row['expirydate'],
            current_date=row['dt'],
            cp=row['cp']
        ), axis=1
    )
    return df

def proc_opt():
    df = pd.read_csv(OPT_INPUT_FILE, thousands=',')
    df['dt'] = pd.to_datetime(df['dt'])

    # extract option info
    opt_df = df[['code', 'tradecode', 'spotcode', 'strike', 'expirydate']].drop_duplicates()
    opt_df['cp'] = opt_df['tradecode'].str[6].map({'C': 1, 'P': -1})

    pivot_df = df.pivot_table(index='dt', columns='tradecode', values='last_price')
    pivot_df = downsample_time(pivot_df, interval_sec=60)
    # filter by time: 09:30:00 to 15:00:00
    pivot_df = pivot_df[(pivot_df.index.time >= pd.to_datetime('09:30:00').time()) & 
                        (pivot_df.index.time <= pd.to_datetime('15:00:00').time())]
    pivot_df = pivot_df.reset_index()
    df = pivot_df.melt(id_vars='dt', var_name='tradecode', value_name='last_price')

    df = df.rename(columns={'last_price': 'openp'})
    df['dt'] -= pd.Timedelta(seconds=60)
    df = df.rename(columns={'openp': 'closep'})

    # filter by time: 09:30:00 to 15:00:00
    df = df[(df['dt'].dt.time >= pd.to_datetime('09:30:00').time()) &
            (df['dt'].dt.time <= pd.to_datetime('15:00:00').time())]
    # filter out lines with closep is NaN
    df = df.loc[~df['closep'].isna()]

    df = df.merge(opt_df, on='tradecode', how='left')
    # filter lines with expirydate > dt
    df = df.loc[df['expirydate'] > df['dt']]

    df = df.rename(columns={
        'spotcode': 'rootcode',
    })
    df = df[['dt', 'code', 'tradecode', 'closep', 
             'rootcode', 'strike', 'expirydate', 'cp']]
    df.to_csv(OPT_DSP_OUTPUT_FILE, index=False)
    print(df.head())
    print(df.tail())
    return df

def proc_opt_greeks():
    df = pd.read_csv(OPT_DSP_OUTPUT_FILE)
    df['dt'] = pd.to_datetime(df['dt'])
    df['expirydate'] = pd.to_datetime(df['expirydate'])
    # df['cp'] = df['tradecode'].str[6].map({'C': 1, 'P': -1})
    df['cp'] = df['cp'].astype(int)

    spot_df = pd.read_csv(SPOT_DSP_OUTPUT_FILE)
    spot_df['dt'] = pd.to_datetime(spot_df['dt'])
    spot_df = spot_df[['dt', 'price']]
    spot_df = spot_df.rename(columns={'price': 'spot_price'})
    df = df.merge(spot_df, on=['dt'], how='left')
    print(df.head())
    print(df.tail())

    # 计算 delta
    # df = df.iloc[:1000]
    df = df_delta(df)

    # 保存结果
    # save with at most 6 decimal places
    df['delta'] = df['delta'].round(6)
    df.to_csv(OPT_GREEKS_OUTPUT_FILE, index=False)
    print(df.head())
    print(df.tail())

def concat_opt_greeks():
    all_files = glob.glob(f'{DATA_DIR}/input/opt_159915_2025_*_greeks.csv')
    df_list = [pd.read_csv(f) for f in all_files]
    concat_df = pd.concat(df_list, ignore_index=True)
    concat_df.to_csv(OPT_GREEKS_CONCAT_OUTPUT_FILE, index=False)

def proc_spot():
    df = pd.read_csv(SPOT_INPUT_FILE)
    df['dt'] = pd.to_datetime(df['dt'])
    df = df.set_index('dt')
    df = downsample_time(df, interval_sec=60)
    df = df.reset_index()

    # filter by time: 09:30:00 to 15:00:00
    df = df[(df['dt'].dt.time >= pd.to_datetime('09:30:00').time()) & 
            (df['dt'].dt.time <= pd.to_datetime('15:00:00').time())]

    df = df.rename(columns={'last_price': 'price'})
    df = df[['dt', 'code', 'price']]
    df.to_csv(SPOT_DSP_OUTPUT_FILE, index=False)
    print(df.head())
    print(df.tail())

if __name__ == '__main__':
    proc_opt()
    # proc_spot()
    proc_opt_greeks()
    concat_opt_greeks()

