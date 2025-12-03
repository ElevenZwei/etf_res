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

create index if not exists idx_future_option_trade_username_id
    on cpr.future_option_trade (username, id);

create extension if not exists pg_trgm;
create index if not exists idx_future_option_trade_tradecode_gist
    on cpr.future_option_trade using gist (tradecode gist_trgm_ops);

COMMIT;
