import click
import datetime
import sqlalchemy as sa
import pandas as pd
from typing import Optional
from dateutil.relativedelta import relativedelta

from config import DATA_DIR, PG_CPR_CONN_INFO, PG_OI_CONN_INFO, get_engine

engine_cpr = get_engine(PG_CPR_CONN_INFO)
engine_oi = get_engine(PG_OI_CONN_INFO)
USE_NEW_DB = False


def fetch_spot_md_old(spot: str, dt_bg: datetime.date, dt_ed: datetime.date) -> pd.DataFrame:
    query = sa.text("""
        select dt, code, last_price
        from market_data_tick
        where code = :spotcode
        and dt >= :dt_bg and dt < :dt_ed
        order by dt asc
    """)
    with engine_oi.connect() as conn:
        df = pd.read_sql(query, conn, params={
            "spotcode": spot,
            "dt_bg": dt_bg,
            "dt_ed": dt_ed + datetime.timedelta(days=1)})
    return df


def fetch_spot_md_new(spot: str, dt_bg: datetime.date, dt_ed: datetime.date) -> pd.DataFrame:
    query = sa.text("""
                    select dt, code, last_price
                    from cpr.contract_price_tick
                    where code = :spotcode
                    and dt >= :dt_bg and dt < :dt_ed
                    order by dt asc
                    """)
    with engine_cpr.connect() as conn:
        df = pd.read_sql(query, conn, params={
            "spotcode": spot,
            "dt_bg": dt_bg,
            "dt_ed": dt_ed + datetime.timedelta(days=1)})
    return df


@click.command()
@click.option('-s', '--spot', required=True, type=str, help='Spot code, e.g., 159915')
@click.option('-b', '--dt_bg', required=False, type=str, help='Begin date in YYYY-MM-DD format')
@click.option('-e', '--dt_ed', required=False, type=str, help='End date in YYYY-MM-DD format')
def click_main(spot: str, dt_bg: Optional[str] = None, dt_ed: Optional[str] = None):
    if dt_bg is None:
        dt_bg_date = datetime.date.today() - relativedelta(months=6)
    else:
        dt_bg_date = datetime.datetime.strptime(dt_bg, "%Y-%m-%d").date()
    if dt_ed is None:
        dt_ed_date = datetime.date.today()
    else:
        dt_ed_date = datetime.datetime.strptime(dt_ed, "%Y-%m-%d").date()
    print(f"Fetching spot market data for {spot} from {dt_bg_date} to {dt_ed_date}")
    if USE_NEW_DB:
        df_spot = fetch_spot_md_new(spot, dt_bg_date, dt_ed_date)
    else:
        df_spot = fetch_spot_md_old(spot, dt_bg_date, dt_ed_date)
    print(f"Fetched {len(df_spot)} rows of spot market data")
    # Save to CSV
    output_file = DATA_DIR / 'fact' / f'spot_md_{spot}_{dt_bg_date.strftime("%Y%m%d")}_{dt_ed_date.strftime("%Y%m%d")}.csv'
    df_spot.to_csv(output_file, index=False)
    print(f"Saved spot market data to {output_file}")


if __name__ == "__main__":
    click_main()
