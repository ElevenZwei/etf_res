import sqlalchemy
from dataclasses import dataclass

@dataclass(frozen=True)
class PgConfig:
    user: str = None
    pw: str = None
    host: str = None
    port: int = None
    db: str = None

PG_DB_CONF = PgConfig(
        user='option',
        pw='option',
        host='localhost',
        port=5432,
        db='opt',
)
def get_engine():
    return sqlalchemy.create_engine(sqlalchemy.URL.create(
        'postgresql+psycopg2',
        username=PG_DB_CONF.user,
        password=PG_DB_CONF.pw,
        host=PG_DB_CONF.host,
        port=PG_DB_CONF.port,
        database=PG_DB_CONF.db,
    ))

