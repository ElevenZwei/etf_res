-- Deploy cpr:014_hb_record to pg

BEGIN;

-- XXX Add DDLs here.

-- 交易记录表格，用于记录每一笔交易的详细信息。
-- trade_id 是交易所返回的全局唯一标识符，每日会从头重新开始。
-- trade_date 和 dt 的区别是 trade_date 是交易所规则下的交易日，可能和 dt::date 不同。

-- "insert into hb.trade_record("
--         "dt, trade_date, username, "
--         "trade_id, order_ref, order_sys_id, "
--         "contract_tradecode, contract_name, underlying, "
--         "direction, open_close, offset_flag, amount, price)"

create table if not exists hb.trade_record (
    id serial primary key,
    dt timestamptz not null default now(),
    trade_date date not null, -- 交易发生的日期，方便查询
    username text not null,
    trade_id int8 not null,
    order_sys_id int8,
    order_ref integer,
    contract_tradecode text not null,
    contract_name text,
    underlying text,
    direction int2 not null,  -- 1 买入，-1 卖出
    open_close int2 not null, -- 1 开仓，-1 平仓
    offset_flag text not null,  -- OPEN, CLOSE, CLOSE_TODAY, CLOSE_YESTERDAY
    amount integer not null,
    price float8 not null
);
-- 这个索引用于快速查询某一天的所有交易记录。
create unique index if not exists idx_trade_record_date_trade_id
    on hb.trade_record(trade_date, trade_id);
create index if not exists idx_trade_record_dt_username_contract
    on hb.trade_record(dt, username, contract_tradecode);


create table if not exists hb.position_record (
    id serial primary key,
    dt timestamptz not null default now(),
    trade_date date not null, -- 持仓记录的日期，方便查询
    username text not null,
    contract_tradecode text not null,
    contract_name text,
    underlying text,
    today_long integer,
    today_short integer,
    history_long integer,
    history_short integer
);
create unique index if not exists idx_position_record_dt_username_contract
    on hb.position_record(dt, username, contract_tradecode);


create table if not exists hb.account_record (
    id serial primary key,
    dt timestamptz not null default now(),
    trade_date date not null, -- 账户记录的日期，方便查询
    username text not null,
    net_worth float8,
    cash float8,
    margin float8,
    position_worth float8,
    deposit float8,
    withdraw float8,
    fee float8
);
create unique index if not exists idx_account_record_dt_username
    on hb.account_record(dt, username);


create table if not exists hb.order_record (
    id serial primary key,
    dt timestamptz not null default now(),
    trade_date date not null, -- 订单记录的日期，方便查询
    username text not null,
    order_ref integer not null, -- 用户下单时的本地唯一标识符
    order_sys_id int8,
    contract_tradecode text not null,
    contract_name text,
    underlying text,
    direction int2 not null,  -- 1 买入，-1 卖出
    open_close int2 not null, -- 1 开仓，-1 平仓
    -- OPEN, CLOSE, CLOSE_TODAY, CLOSE_YESTERDAY
    offset_flag text not null,  
    -- LIMIT, MARKET, IOC, FOK, LIMIT_FIVE_LEVEL, IOC_FIVE_LEVEL
    order_type text not null,  
    limit_price float8,
    order_amount integer not null,
    filled_amount integer not null,
    -- RISK_BLOCK, ACCEPTED, REJECTED, PARTIAL_FILLED, FILLED, PARTIAL_CANCELED, CANCELED
    order_status text not null,  
    error_code int,
    is_canceled boolean not null default false
);
-- 这里不能设置唯一约束，因为这是一个日志类型的表格，
-- 允许多次更新同一个订单的状态。
create index if not exists idx_order_record_dt_username_order_ref
    on hb.order_record(dt, username, order_ref);
create index if not exists idx_order_record_username_contract_name
    on hb.order_record(username, contract_name);



COMMIT;
