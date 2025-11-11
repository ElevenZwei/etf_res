import pandas as pd
import datetime

from config import DATA_DIR, get_engine, upsert_on_conflict_skip

engine = get_engine()

def import_stock_signal_history(fpath: str):
    basename = fpath.split('/')[-1].replace('.parquet', '')
    df = pd.read_parquet(fpath)
    # set index name to dt
    df.index.name = 'dt'
    df = df.reset_index()
    df['name'] = basename
    df['acname'] = basename
    df['if_final'] = 1
    df['product'] = '399006'
    df.rename(columns={
        '399006': 'ps',
        'dt': 'insert_time',
    }, inplace=True)
    print(f"Read {len(df)} rows from {fpath}.")

    # df_nulls = df.loc[df['ps'].isnull()]
    # print(df_nulls)
    # forward fill in the same day
    df['ps'] = df.groupby(df['insert_time'].dt.date)['ps'].ffill().fillna(0)
    # print(df.tail())
    # fill ps to 0 if insert_time after 14:51
    df['ps'] = df.apply(
        lambda row: 0 if row['insert_time'].time() >= datetime.time(14, 51) else row['ps'],
        axis=1
    )
    print(df.loc[df['insert_time'].dt.date == datetime.date(2025, 10, 14)])
    df_concise = df[['acname', 'insert_time', 'ps', 'product']]
    df_concise = df_concise.to_csv(f'{DATA_DIR}/signal/cyb_position/{basename}_concise.csv', index=False)
    return df

    # df.to_sql('stock_signal', engine,
    #         if_exists='append', index=False,
    #         method=upsert_on_conflict_skip,
    #         chunksize=1000)
    # print(f"Imported {len(df)} rows to cpr.stock_signal.")

def load_stock_signal_from_db(name: str, dt_from: datetime.date, dt_to: datetime.date) -> pd.DataFrame:
    query = f"""
        select insert_time as dt, ps as position
        from cpr.stock_signal
        where product = '399006'
            and acname = :name
            and insert_time >= :dt_from
            and insert_time <= :dt_to
        order by insert_time;
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'name': name,
            'dt_from': dt_from,
            'dt_to': dt_to
        })
    return df



if __name__ == '__main__':
    # import_stock_signal_history(f'{DATA_DIR}/signal/cyb_position/pyelf_CybWoOacVsw_sif_1_1.parquet')
    import_stock_signal_history(f'{DATA_DIR}/signal/cyb_position/pyelf_CybWoOacVswRmRR_sif_1_1.parquet')
