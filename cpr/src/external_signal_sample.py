import pandas as pd
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
product = '399006_test'

# 准备测试数据
df = pd.DataFrame({
    'product': [product] * 5,
    'acname': ['1', '1', '2', '3', '3'],
    'insert_time': pd.to_datetime([
        '2025-06-10 10:01:00',
        '2025-06-10 10:02:00',
        '2025-06-10 10:01:00',
        '2025-06-10 10:03:00',
        '2025-06-10 10:02:00',
    ]),
    'ps': [-1, -0.5, 0, 0.5, 1],
    'if_final': [False, False, False, False, True],
})

# 这里提供两种写入数据库的方法
# 写入方法 1 - to_sql
df.to_sql('stock_signal', engine, schema='cpr',
        if_exists='append', index=False,
        chunksize=1000,)

# 写入方法 2 - raw sql
query = sqlalchemy.text("""
    insert into cpr.stock_signal
    (product, acname, insert_time, ps, if_final)
    values (:product, :acname, :insert_time, :ps, :if_final)
""")
with engine.connect() as conn:
    for tup in df.itertuples(index=False):
        conn.execute(query, {
            'product': tup.product,
            'acname': tup.acname,
            'insert_time': tup.insert_time,
            'ps': tup.ps,
            'if_final': tup.if_final,
        })
    conn.commit()  # Or it will rollback

# 读取全表验证
query = sqlalchemy.text("""
    select * from cpr.stock_signal where product = :product
""")
with engine.connect() as conn:
    df2 = pd.read_sql(query, conn, params={
        'product': product
    })
print(df2)

# 读取最新一条记录验证
query = sqlalchemy.text("""
    select distinct on (acname) acname, insert_time, ps
    from cpr.stock_signal
    where product = :product
    order by acname, insert_time desc
""")
with engine.connect() as conn:
    df3 = pd.read_sql(query, conn, params={
        'product': product
    })
print(df3)

# 清理测试数据
query = sqlalchemy.text("""
    delete from cpr.stock_signal where product = :product
""")
with engine.connect() as conn:
    conn.execute(query, {
        'product': product
    })
    conn.commit()  # Or it will rollback

