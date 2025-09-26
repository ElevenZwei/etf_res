import sqlalchemy
from sqlalchemy.dialects import postgresql
from dataclasses import dataclass
from pathlib import Path

def get_file_dir():
    fpath = Path(__file__).resolve()
    return fpath.parent

DATA_DIR = get_file_dir() / '..' / 'data'

@dataclass(frozen=True)
class PgConfig:
    user: str
    pw: str
    host: str
    port: int
    db: str

PG_CPR_CONN_INFO = PgConfig(
        user='option',
        pw='option',
        host='localhost',
        port=5432,
        db='opt',
)

PG_OI_CONN_INFO = PgConfig(
        user='option',
        pw='option',
        host='localhost',
        port=15432,
        db='opt',
)

def get_engine(config: PgConfig = PG_CPR_CONN_INFO, timeout: int = 40):
    return sqlalchemy.create_engine(sqlalchemy.URL.create(
        'postgresql',
        username=config.user,
        password=config.pw,
        host=config.host,
        port=config.port,
        database=config.db,
        query={
            'sslmode': 'require' if config.host != 'localhost' else 'disable',
            'connect_timeout': str(timeout),
        },
    ))

def upsert_on_conflict_skip(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = postgresql.insert(table.table).values(data)
    stmt = stmt.on_conflict_do_nothing()
    conn.execute(stmt)

import json
import numpy as np

class NpEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, np.integer):
            return int(o)
        if isinstance(o, np.floating):
            return float(o)
        if isinstance(o, np.ndarray):
            return o.tolist()
        return super().default(o)

