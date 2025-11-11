"""
Download contract information from Open Interest database and save as CSV files.
"""

import click
import datetime
import os
import sqlalchemy as sa
import pandas as pd

from config import DATA_DIR, PG_CPR_CONN_INFO, PG_OI_CONN_INFO, get_engine

CONTRACT_INFO_DIR = os.path.join(DATA_DIR, "fact/contract_info")
USE_NEW_DB = True

if not os.path.exists(CONTRACT_INFO_DIR):
    os.makedirs(CONTRACT_INFO_DIR, exist_ok=True)

def get_engine_wrapper() -> sa.engine.Engine:
    return get_engine(PG_CPR_CONN_INFO if USE_NEW_DB else PG_OI_CONN_INFO)


def dl_contract_info(spot: str, dt: datetime.date) -> pd.DataFrame:
    """ Download option contract information of given spot with nearest expiry after given date. """
    global USE_NEW_DB
    if dt <= datetime.date(2025, 10, 23):
        USE_NEW_DB = False
    if USE_NEW_DB:
        df = fetch_contract_info_new(spot, dt)
    else:
        df = fetch_contract_info_old(spot, dt)
    return df


def fetch_contract_info_old(spot: str, dt: datetime.date) -> pd.DataFrame:
    """ Fetch option contracts of given spot with nearest expiry after given date from old OI database. """
    query = sa.text("""
            with nearest_expiry as (
                select min(expirydate) as expiry
                from contract_info
                where spotcode = :spot and expirydate >= :dt
            )
            select
                    code as tradecode, tradecode as name,
                    spotcode, strike, callput,
                    expirydate as expiry,
                    contractunit as lot_size
            from contract_info
            where spotcode = :spot and expirydate = (select expiry from nearest_expiry)
    """)
    with get_engine_wrapper().connect() as conn:
        df = pd.read_sql(query, conn, params={"spot": spot, "dt": dt})
    return df

def fetch_contract_info_new(spot: str, dt: datetime.date) -> pd.DataFrame:
    """ Fetch option contracts of given spot with nearest expiry after given date from old OI database. """
    query = sa.text("""
            with nearest_expiry as (
                select min(expiry) as expiry
                from contract_info
                where spotcode = :spot and expiry >= :dt
            )
            select
                    tradecode, name,
                    spotcode, strike, callput,
                    expiry, lot_size
            from contract_info
            where spotcode = :spot and expiry = (select expiry from nearest_expiry)
    """)
    with get_engine_wrapper().connect() as conn:
        df = pd.read_sql(query, conn, params={"spot": spot, "dt": dt})
    return df


@click.command()
@click.argument('spot')
@click.argument('dt', type=click.DateTime(formats=["%Y-%m-%d"]))
def cli(spot: str, dt: datetime.datetime):
    """ Download option contract information of given spot with nearest expiry after given date. """
    df = dl_contract_info(spot, dt.date())
    file_path = os.path.join(CONTRACT_INFO_DIR, f"contract_info_{spot}_{dt.date().isoformat()}.csv")
    df.to_csv(file_path, index=False)
    print(f"Contract information saved to {file_path}")

if __name__ == "__main__":
    cli()

# df = fetch_contract_info_old('159915', datetime.date(2025, 9, 23))
# print(df)
# df.to_csv('test_contract_info.csv', index=False)
#
