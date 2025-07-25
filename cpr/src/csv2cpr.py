import pandas as pd
import sqlalchemy

from config import get_engine, upsert_on_conflict_skip

INPUT_DIR = "../data/fact/"

OI_CSV = {
        '159915': 'oi_159915_20250101_20250709.csv',
        '510500': 'oi_510500.SH_2024.csv',
        # '510500': 'oi_510500_20250101_20250710.csv',
}

engine = get_engine()
metadata = sqlalchemy.MetaData()
dataset_table = sqlalchemy.Table('dataset', metadata, autoload_with=engine, schema='cpr')

def get_dataset_id(spotcode: str, expiry_priority: int, strike: float):
    # insert a new row and return the id, or return the existing id if it already exists
    stmt = (sqlalchemy.dialects.postgresql.insert(dataset_table)
        .values(spotcode=spotcode, expiry_priority=expiry_priority, strike=strike)
        .on_conflict_do_nothing(index_elements=['spotcode', 'expiry_priority', 'strike'])
        .returning(dataset_table.c.id))
    with engine.begin() as conn:
        result = conn.execute(stmt)
        dataset_id = result.scalar()
        if dataset_id is None:
            # If the row was not inserted, fetch the existing id
            stmt = sqlalchemy.select(dataset_table.c.id).where(
                sqlalchemy.and_(
                    dataset_table.c.spotcode == spotcode,
                    dataset_table.c.expiry_priority == expiry_priority,
                    dataset_table.c.strike == strike
                ))
            result = conn.execute(stmt)
            dataset_id = result.scalar()
    return dataset_id


def downsample_time(df: pd.DataFrame, interval_sec: int):
    df = df.resample(f'{interval_sec}s').first()
    # 这里跳过没有开盘的时间
    df = df.loc[~df.isna().all(axis=1)]
    return df

def upload_csv_to_cpr(spot: str, csv_file: str):
    df = pd.read_csv(INPUT_DIR + csv_file)
    df = df.set_index(pd.to_datetime(df['dt']))
    df = downsample_time(df, 60)  # downsample to 1 minute
    df = df[['call_oi_sum', 'put_oi_sum']]
    df = df.rename(columns={'call_oi_sum': 'call', 'put_oi_sum': 'put'})
    df['call'] = df['call'].ffill()
    df['put'] = df['put'].ffill()
    df = df.reset_index()
    dataset_id = get_dataset_id(spot, 1, 0.0)
    df['dataset_id'] = dataset_id
    print(df.head())
    df.to_sql('cpr', engine, schema='cpr',
              if_exists='append', index=False,
              method=upsert_on_conflict_skip,
              chunksize=1000)


# upload_csv_to_cpr('159915', OI_CSV['159915'])
upload_csv_to_cpr('510500', OI_CSV['510500'])

