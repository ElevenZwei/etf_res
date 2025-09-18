-- Deploy cpr:011_md to pg
-- 这个文件的目标是保存市场数据相关的信息。
-- 包括交易所信息、合约信息、行情数据等。
-- 设计目标是尽量节省存储空间和计算时间，把大部分的计算工作在录制软件里完成。
-- 它需要数据库预装 timescaledb 扩展，以便高效存储和查询时间序列数据。

BEGIN;

-- XXX Add DDLs here.

create schema if not exists md;
-- postgres need many grants to use the md schema.
-- schema level grants
grant usage, create on schema md to option;
grant select, insert, update, delete on all tables in schema md to option;
grant usage, update on all sequences in schema md to option;
grant execute on all functions in schema md to option;

-- table level grants
alter default privileges in schema md
    grant select, insert, update, delete on tables to option;
alter default privileges in schema md
    grant usage, update on sequences to option;
alter default privileges in schema md
    grant execute on functions to option;

alter role option set search_path to md, md, cpr, public;

-- 交易所信息表，保存交易所的基本信息
create table if not exists md.exchange_info (
    exchange text primary key,
    name text not null,
    timezone text,
    is_commodity boolean,
    updated_at timestamptz not null default now()
);

-- 合约信息表，保存合约的基本信息
-- 暂时只考虑期权，期货，和股票的需求。
-- 对应的更新函数在 upsert_contract_info() .
create table if not exists md.contract_info (
    tradecode text primary key,
    name text not null,
    exchange text not null references md.exchange_info(exchange),
    -- 每手合约的数量
    lot_size integer not null,
    -- 标记期权类型，不使用 null 是为了可以索引。
    -- 1 表示看涨期权，-1 表示看跌期权，0 表示非期权
    callput integer not null,
    -- 非期权合约为 null
    spotcode text,
    -- 期权链标记代码
    chaincode text,
    -- 期权行权价，非期权合约为 null，
    -- 记录真实的行权价，而非名称中的行权价
    strike float8,
    -- 期权到期日
    expiry date,
    -- updated_at 用于记录该合约信息的最后更新时间
    -- 例如，有些合约会变更名称、行权价等信息
    -- 通常来说这个时间是程序第一次抓取到该合约信息的时间
    updated_at timestamptz not null default now()
);
-- name 可能在某些时候会有短时间的重复。
create index if not exists idx_contract_info_name
    on md.contract_info (name);
create index if not exists idx_contract_spotcode_expiry
    on md.contract_info (spotcode, expiry);
create index if not exists idx_contract_chaincode
    on md.contract_info (chaincode);

-- 合约信息历史表，保存合约的历史信息，用于追溯合约信息的变更
-- 每次合约信息变更时，插入一条新记录
-- 通过 tradecode + updated_at 唯一标识一条记录
-- updated_at 表示该记录的生效时间
-- 例如，有些合约会变更名称、行权价等信息
create table if not exists md.contract_info_history (
    id serial primary key,
    tradecode text not null,
    name text not null,
    exchange text references md.exchange_info(exchange),
    spotcode text,
    lot_size integer, -- 每手合约的数量
    chaincode text, -- 期权链标记代码
    strike float8, -- 期权行权价，非期权合约为 null，记录真实的行权价，而非名称中的行权价
    callput integer, -- 1 表示看涨期权， -1 表示看跌期权， 0 表示非期权
    expiry date, -- 期权到期日
    updated_at timestamptz not null default now()
);
create unique index if not exists idx_contract_info_history_tradecode_updated_at
    on md.contract_info_history (tradecode, updated_at desc);

-- 日线级别的实时市场行情数据，使用 tradecode + dt 唯一标识一条记录
-- 使用 float8 ，节省存储空间和计算时间。
-- vol, oi 保存当日最后更新的数值，和 close price 同步。
-- 更新频次按市场推送频率并且这个合约有新的数据变化时更新。
-- update 频率较高，避开 insert 操作，避免 serial id 成为瓶颈。
create table if not exists md.contract_price_daily (
    id serial,
    tradecode text not null references md.contract_info(tradecode),
    dt date not null,
    open float8,
    high float8,
    low float8,
    close float8,
    vol bigint,
    oi integer,
    days_left integer, -- 距离到期日的天数，expiry - dt + 1
    updated_at timestamptz not null default now()
);
select create_hypertable('md.contract_price_daily', 'dt', if_not_exists => true);
-- 避免 insert 操作，同时保证 update 不改变任何索引数据列。
create unique index if not exists idx_contract_price_daily_id
    on md.contract_price_daily (id, dt);
create unique index if not exists idx_contract_price_daily_tradecode_dt
    on md.contract_price_daily (tradecode, dt desc);

-- 分钟级别的实时市场行情数据，使用 tradecode + dt 唯一标识一条记录
-- 使用 float8 ，节省存储空间和计算时间。
-- vol, oi 保存这一分钟里最后更新的数值，和 close price 同步。
-- dt 对齐到分钟，例如 2023-01-01 09:31:00+08
-- 更新频次按市场推送频率并且这个合约有新的数据变化时更新。
-- update 频率较高，避开 insert 操作，避免 serial id 成为瓶颈。
create table if not exists md.contract_price_minute (
    id serial,
    tradecode text not null references md.contract_info(tradecode),
    dt timestamptz not null,
    open float8,
    high float8,
    low float8,
    close float8,
    vol bigint,
    oi integer,
    updated_at timestamptz not null default now()
);
select create_hypertable('md.contract_price_minute', 'dt', if_not_exists => true);
-- 避免 insert 操作，同时保证 update 不改变任何索引数据列。
create unique index if not exists idx_contract_price_minute_id
    on md.contract_price_minute (id, dt);
create unique index if not exists idx_contract_price_minute_tradecode_dt
    on md.contract_price_minute (tradecode, dt desc);

-- Tick 级别的实时市场行情数据，不使用统一的 id 字段，因为高速插入 serial id 会成为瓶颈。
-- Tick 级别数据量大，大部分使用 tradecode + dt 唯一标识一条记录。
-- dt 精确到毫秒，例如 2023-01-01 09:31:00.123+08
-- 更新频次按市场推送频率并且这个合约有新的数据变化时更新。
-- 基本采用 copy 插入数据，避免 insert 语句的解析开销。

-- Tick 级别的实时市场行情数据，使用 tradecode + dt 唯一标识一条记录
-- 使用 float8 ，节省存储空间和计算时间。保存三档买卖盘。
create table if not exists md.contract_price_tick (
    tradecode text not null references md.contract_info(tradecode),
    dt timestamptz not null,
    last_price float8,
    vol bigint,
    oi integer,
    ask_price float8, bid_price float8,
    ask_size integer, bid_size integer,
    ask2_price float8, bid2_price float8,
    ask2_size integer, bid2_size integer,
    ask3_price float8, bid3_price float8,
    ask3_size integer, bid3_size integer
);
select create_hypertable('md.contract_price_tick', 'dt', if_not_exists => true);
create unique index if not exists idx_contract_price_tick_tradecode_dt
    on md.contract_price_tick (tradecode, dt desc);

-- Tick 级别的期权希腊字母数据，使用 tradecode + dt 唯一标识一条记录
-- 更新频次按市场推送频率并且这个合约有新的数据变化时更新。
create table if not exists md.option_greeks_tick (
    tradecode text not null references md.contract_info(tradecode),
    dt timestamptz not null,
    price_level int2, -- 价格层级，1 表示 ask 1, -1 表示 bid 1， 0 表示 mid
    time_value float8, -- 时间价值
    intrinsic_value float8, -- 内在价值
    delta float8,
    gamma float8,
    theta float8,
    vega float8,
    rho float8,
    iv float8
);
select create_hypertable('md.option_greeks_tick', 'dt', if_not_exists => true);
create unique index if not exists idx_option_greeks_tick_tradecode_dt
    on md.option_greeks_tick (tradecode, dt desc);

-- Tick 级别的逐笔成交数据，使用 tradecode + dt 唯一标识一条记录
-- Ctp Options 没有向公众开放即时市场成交数据接口。
-- 只有他们自己的客户端软件可以看到逐笔成交数据。
-- 所以暂时不保存 ETF 期权的逐笔成交数据。
create table if not exists md.contract_trade_tick (
    tradecode text not null references md.contract_info(tradecode),
    dt timestamptz not null,
    price float8,
    amount integer,
    trade_type int2 -- 1 表示买入， -1 表示卖出， 0 表示中性
);
select create_hypertable('md.contract_trade_tick', 'dt', if_not_exists => true);
create unique index if not exists idx_contract_trade_tick_tradecode_dt
    on md.contract_trade_tick (tradecode, dt desc);

-- Tick 级别的 Strike Level Put Call Parity 数据，
-- 使用 chaincode + strike + dt 唯一标识一条记录
create table if not exists md.put_call_parity_strike_tick (
    -- 期权链标记代码
    chaincode text not null,
    strike float8 not null, -- 行权价
    dt timestamptz not null,
    call_parity float8, -- buy call, sell put, the time value difference
    put_parity float8  -- buy put, sell call, the time value difference
);
select create_hypertable('md.put_call_parity_strike_tick', 'dt', if_not_exists => true);
create unique index if not exists idx_put_call_parity_strike_tick_chaincode_strike_dt
    on md.put_call_parity_strike_tick (chaincode, strike, dt desc);

-- Tick 级别的 Chain Level Put Call Parity 数据，
-- 使用 chaincode + dt 唯一标识一条记录
create table if not exists md.put_call_parity_chain_tick (
    -- 期权链标记代码
    chaincode text not null,
    dt timestamptz not null,
    call_parity float8, -- buy call, sell put, the time value difference
    put_parity float8  -- buy put, sell call, the time value difference
);
select create_hypertable('md.put_call_parity_chain_tick', 'dt', if_not_exists => true);
create unique index if not exists idx_put_call_parity_chain_tick_chaincode_dt
    on md.put_call_parity_chain_tick (chaincode, dt desc);


COMMIT;
