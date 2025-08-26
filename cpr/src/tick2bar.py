import pandas as pd
import sqlalchemy

from config import get_engine, upsert_on_conflict_skip

INPUT_DIR = "../data/fact/"

# 从期权数据里面提取不是很稳定，因为可能有一分钟期权完全不交易。
# 最好还是从 market_data_tick 里面提取。
OI_CSV = {
        # '159915': 'oi_159915_20250101_20250709.csv',
        # '510500': 'oi_510500_20250101_20250710.csv',
        '159915': 'oi_merge/oi_159915.csv',
}


def tick2bar(series: pd.Series) -> pd.DataFrame:
    """
    Convert a Series of ticks to a DataFrame of bars.
    The Series should have a datetime index and a 'price' column.
    """
    df = series.to_frame(name='price')
    df.index.name = 'dt'
    # Resample to 1 minute bars with open, high, low, close
    df = df.resample('1min').agg({
        'price': ['first', 'max', 'min', 'last']
    }).dropna()
    df.columns = ['openp', 'highp', 'lowp', 'closep']
    return df.reset_index()


def convert_df_to_bars(spot: str, df: pd.DataFrame):
    df['dt'] = pd.to_datetime(df['dt'])
    df.set_index('dt', inplace=True)
    # Convert to bars
    bar_df = tick2bar(df['spot_price'])
    # Add spotcode and dataset_id
    bar_df['code'] = spot
    print(bar_df.head())
    # return
    # Save to database
    bar_df.to_sql('market_minute', get_engine(), schema='cpr',
            if_exists='append', index=False,
            method=upsert_on_conflict_skip,
            chunksize=1000,)
    return bar_df


def convert_csv_to_bars(spot: str, csv_file: str):
    df = pd.read_csv(INPUT_DIR + csv_file)
    return convert_df_to_bars(spot, df)


if __name__ == "__main__":
    convert_csv_to_bars('159915', OI_CSV['159915'])
    # convert_csv_to_bars('510500', OI_CSV['510500'])
