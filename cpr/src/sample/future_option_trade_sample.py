import polars as pl
import sqlalchemy
from dataclasses import dataclass

@dataclass(frozen=True)
class PgConfig:
    user: str
    pw: str
    host: str
    port: int
    db: str

# 数据库连接配置
# 有两个地址，一个内网地址，一个公网地址
# 内网地址是 172.16.30.6:5432
# 公网地址是 124.222.94.46:19018
# 内网地址连接更快更稳定，先尝试内网地址，失败后再尝试公网地址
PG_CONN_INFO = PgConfig(
        user='option',
        pw='option',
        host='172.16.30.6',
        port=5432,
        # host='124.222.94.46',
        # port=19018,
        db='opt',
)

def get_engine(config: PgConfig = PG_CONN_INFO, timeout: int = 10):
    return sqlalchemy.create_engine(sqlalchemy.URL.create(
        'postgresql',
        username=config.user,
        password=config.pw,
        host=config.host,
        port=config.port,
        database=config.db,
        query={
            'sslmode': 'disable',
            'connect_timeout': str(timeout),
        },
    ))

engine = get_engine()

"""
Table Schema:

create table if not exists cpr.future_option_trade (
    id serial primary key,
    dt timestamptz not null default now(),
    username text not null,
    tradecode text not null,
    direction int2 not null,  -- 1 买入，-1 卖出
    amount integer not null,
    price float8 not null,
    fee float8 not null,
    reason text
);

"""

# 准备测试数据
df = pl.DataFrame({
    'username': ['test_user'] * 3,
    'tradecode': ['ps2601-C-63000', 'ps2601-P-54000', 'ps2601-C-64000'],
    'direction': [1, -1, 1],
    'amount': [1, 2, 1],
    'price': [12.5, 8.3, 15.0],
    'fee': [8, 8, 8],
    'reason': ['test buy call', 'test sell put', 'test buy call 2'],
})

# 这里提供几种不同的写入数据库的方法。
# 1 - polars write_database
with engine.connect() as conn:
    df.write_database(
            'cpr.future_option_trade',
connection=conn,
            if_table_exists='append')

# 2 - pandas to_sql
pd_df = df.to_pandas()
pd_df.to_sql('future_option_trade', engine, schema='cpr',
            if_exists='append', index=False)

# 3 - raw sql
query = sqlalchemy.text("""
    insert into cpr.future_option_trade
    (username, tradecode, direction, amount, price, fee, reason)
    values (:username, :tradecode, :direction, :amount, :price, :fee, :reason)
    """)
with engine.connect() as conn:
    for row in df.iter_rows(named=True):
        conn.execute(query, parameters={
            'username': row['username'],
            'tradecode': row['tradecode'],
            'direction': row['direction'],
            'amount': row['amount'],
            'price': row['price'],
            'fee': row['fee'],
            'reason': row['reason'],
        })
    conn.commit()  # Or it will rollback


# 现在应该插入了三份测试数据，我们来读取验证一下
query = sqlalchemy.text("""
    select * from cpr.future_option_trade where username = :username;
""")
with engine.connect() as conn:
    df_validate = pl.read_database(query, conn, execute_options={
        'parameters': {
            'username': 'test_user'
        }
    })
print(df_validate)

# 清理测试数据
query = sqlalchemy.text("""
    delete from cpr.future_option_trade where username = :username;
""")
with engine.connect() as conn:
    conn.execute(query, {
        'username': 'test_user'
    })
    conn.commit()  # Or it will rollback

