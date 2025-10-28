import glob
import pandas as pd
import numpy as np
from tqdm import tqdm
from joblib import Parallel, delayed
from backtest.config import DATA_DIR
from backtest.scripts.black_scholes import calculate_d1, calculate_delta, calculate_impv

tqdm.pandas(desc="Processing")

# DATE_SUFFIX = '0102_0527'
# DATE_SUFFIX = '0408_0709'
# DATE_SUFFIX = '0709_0819'
DATE_SUFFIX = '0820_1027'
OPT_INPUT_FILE = f'{DATA_DIR}/db/opt_159915_2025_{DATE_SUFFIX}.csv'
SPOT_INPUT_FILE = f'{DATA_DIR}/db/spot_159915_2025.csv'
# mid output
OPT_DSP_OUTPUT_FILE = f'{DATA_DIR}/db/opt_159915_2025_{DATE_SUFFIX}_dsp.csv'
OPT_GREEKS_OUTPUT_FILE = f'{DATA_DIR}/db/opt_159915_2025_{DATE_SUFFIX}_greeks.csv'
# final output
SPOT_DSP_OUTPUT_FILE = f'{DATA_DIR}/input/spot_159915_2025_dsp.csv'
OPT_GREEKS_CONCAT_OUTPUT_FILE = f'{DATA_DIR}/input/opt_159915_2025_greeks.csv'


def downsample_time(df: pd.DataFrame, interval_sec: int):
    df1 = df.resample(f'{interval_sec}s').first().dropna(how='all')
    df2 = df.resample(f'{interval_sec}s').last().dropna(how='all')
    return df1, df2


def line_greeks(option_price: float, future_price: float,
                strike: float, expiry_date: pd.Timestamp, 
               current_date: pd.Timestamp, cp: int) -> float:
    """计算期权的 delta"""
    # set expiry_date with timezone
    if expiry_date.tzinfo is None:
        expiry_date = expiry_date.tz_localize('Asia/Shanghai')
    if current_date >= expiry_date + pd.Timedelta(days=1):
        return 0.0
    t = (expiry_date - current_date).days / 365.0  # 转换为年
    if t <= 0:
        return 0.0
    rate = 0.0159
    volatility = calculate_impv(option_price, future_price, strike, rate, t, cp)
    if (volatility is None) or (volatility <= 0):
        if strike > future_price * 1.01:
            if cp == 1:
                return 0.0
            else:
                return -1.0
        elif strike < future_price * 0.99:
            if cp == 1:
                return 1.0
            else:
                return 0.0
        return 0.5 * cp
    d1 = calculate_d1(future_price, strike, rate, t, volatility)
    delta = calculate_delta(future_price, strike, rate, t, volatility, cp, d1)
    return delta


def parallel_progress_apply(df: pd.DataFrame, func, n_jobs=4):
    chunks = np.array_split(df, n_jobs * 10)
    def process_chunk(chunk):
        return chunk.apply(func, axis=1)
    results = Parallel(n_jobs=n_jobs)(
            delayed(process_chunk)(chunk)
            for chunk in tqdm(chunks, desc="Parallel Processing"))
    return pd.concat(results)


def df_delta(df: pd.DataFrame):
    # 逐行计算 delta
    # 每一行的输入是 'dt', 'expirydate', 'strike', 'spot_price', 'cp'
    df['delta'] = parallel_progress_apply(df,
        func=lambda row: line_greeks(
            option_price=row['openp'],
            future_price=row['spot_price'],
            strike=row['strike'],
            expiry_date=row['expirydate'],
            current_date=row['dt'],
            cp=row['cp']
        ), n_jobs=100)
    return df


def proc_opt():
    df = pd.read_csv(OPT_INPUT_FILE, thousands=',')
    df['dt'] = pd.to_datetime(df['dt'])

    def calc_mid_price(row):
        if pd.isna(row['bid_price']) and pd.isna(row['ask_price']):
            return np.nan
        if pd.isna(row['bid_price']):
            return row['ask_price']
        if pd.isna(row['ask_price']):
            return row['bid_price']
        return (row['bid_price'] + row['ask_price']) / 2

    df['mid_price'] = parallel_progress_apply(df,
        func=calc_mid_price,
        n_jobs=80)

    # extract option info
    opt_info_df = df[['code', 'tradecode', 'spotcode', 'strike', 'expirydate']].drop_duplicates()
    opt_info_df['cp'] = opt_info_df['tradecode'].str[6].map({'C': 1, 'P': -1})

    pivot_df = df.pivot_table(index='dt', columns='tradecode', values='mid_price')
    pivot_df1, pivot_df2 = downsample_time(pivot_df, interval_sec=60)

    def proc_pivot(pivot_df):
        # filter by time: 09:30:00 to 15:00:00
        pivot_df = pivot_df[(pivot_df.index.time >= pd.to_datetime('09:30:00').time()) & 
                            (pivot_df.index.time <= pd.to_datetime('15:00:00').time())]
        pivot_df = pivot_df.reset_index()
        df = pivot_df.melt(id_vars='dt', var_name='tradecode', value_name='mid_price')
        return df

    df1 = proc_pivot(pivot_df1).rename(columns={'mid_price': 'openp'})
    df2 = proc_pivot(pivot_df2).rename(columns={'mid_price': 'closep'})
    df = df1.merge(df2, on=['dt', 'tradecode'], how='inner')

    # filter out lines with closep is NaN
    df = df.loc[df[['openp', 'closep']].notna().all(axis=1)]
    df = df.merge(opt_info_df, on='tradecode', how='left')

    # filter lines within expiry date
    df['expirydate'] = pd.to_datetime(df['expirydate'])
    df = df.loc[df['expirydate'] + pd.Timedelta(days=1) > df['dt'].dt.date]

    df = df.rename(columns={
        'spotcode': 'rootcode',
    })
    df = df[['dt', 'code', 'tradecode', 'openp', 'closep', 
             'rootcode', 'strike', 'expirydate', 'cp']]
    df.to_csv(OPT_DSP_OUTPUT_FILE, index=False)
    print('option data processing output:')
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
    spot_df = spot_df[['dt', 'openp']]
    spot_df = spot_df.rename(columns={'openp': 'spot_price'})
    df = df.merge(spot_df, on=['dt'], how='left')
    print('greeks calculation input:')
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
    all_files = glob.glob(f'{DATA_DIR}/db/opt_159915_2025_*_greeks.csv')
    df_list = [pd.read_csv(f) for f in all_files]
    concat_df = pd.concat(df_list, ignore_index=True)
    concat_df.drop_duplicates(subset=['dt', 'code'], inplace=True)
    concat_df.sort_values(by=['tradecode', 'dt'], inplace=True)
    concat_df.to_csv(OPT_GREEKS_CONCAT_OUTPUT_FILE, index=False)


def concat_spot():
    all_files = glob.glob(f'{DATA_DIR}/db/spot_159915_2025_*.csv')
    df_list = [pd.read_csv(f) for f in all_files]
    concat_df = pd.concat(df_list, ignore_index=True)
    concat_df.drop_duplicates(subset=['dt'], inplace=True)
    concat_df.to_csv(SPOT_INPUT_FILE, index=False)


def proc_spot():
    df = pd.read_csv(SPOT_INPUT_FILE)
    df['dt'] = pd.to_datetime(df['dt'])
    df = df.set_index('dt')
    # filter out lines which last_price is NaN
    df = df.loc[df['last_price'].notna()]
    df1, df2 = downsample_time(df, interval_sec=60)
    df1 = df1.rename(columns={'last_price': 'openp'}).reset_index()
    df2 = df2.rename(columns={'last_price': 'closep'}).reset_index()
    df = df1.merge(df2, on=['dt', 'code'], how='inner')
    # filter by time: 09:30:00 to 15:00:00
    df = df[(df['dt'].dt.time >= pd.to_datetime('09:30:00').time()) & 
            (df['dt'].dt.time <= pd.to_datetime('15:00:00').time())]
    # filter by week day: Monday to Friday
    df = df[df['dt'].dt.weekday < 5]
    df = df[['dt', 'code', 'openp', 'closep']]
    df['code'] = df['code'].astype(int).astype(str)
    df.to_csv(SPOT_DSP_OUTPUT_FILE, index=False)
    print(df.head())
    print(df.tail())


if __name__ == '__main__':
    # concat_spot()
    # proc_spot()
    # proc_opt()
    # proc_opt_greeks()
    concat_opt_greeks()
    #
