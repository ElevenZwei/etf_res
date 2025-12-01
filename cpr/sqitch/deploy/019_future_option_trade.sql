-- Deploy cpr:019_future_option_trade to pg

BEGIN;

-- XXX Add DDLs here.
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

create index if not exists idx_future_option_trade_dt_username_tradecode
    on cpr.future_option_trade (dt, username, tradecode);

COMMIT;
