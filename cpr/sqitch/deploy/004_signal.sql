-- Deploy cpr:004_signal to pg

BEGIN;

-- XXX Add DDLs here.
create table cpr.clip_trade_args (
    id serial primary key,
    method_id integer not null references cpr.method(id) on delete cascade,
    date_interval integer not null,
    variation text not null,
    args jsonb not null
);
create unique index on cpr.clip_trade_args (method_id, date_interval, variation);

create table cpr.clip_trade_backtest (
    dt timestamptz not null,
    dataset_id integer not null references cpr.dataset(id) on delete cascade,
    trade_args_id integer not null references cpr.clip_trade_args(id) on delete cascade,
    is_trading boolean not null,
    zone text not null,
    position float8 not null,
    value float8 not null,
    long_open float8 not null,
    long_close float8 not null,
    short_open float8 not null,
    short_close float8 not null
);
create unique index on cpr.clip_trade_backtest (dataset_id, trade_args_id, dt);
create index if not exists cpr_clip_trade_backtest_trade_args_idx
    on cpr.clip_trade_backtest (trade_args_id, dt);
create index on cpr.clip_trade_backtest (dt, dataset_id);
select create_hypertable('cpr.clip_trade_backtest', 'dt',
    chunk_time_interval => interval '1 week',
    create_default_indexes => false);

create or replace function cpr.get_or_create_clip_trade_args(
    method_id_arg integer, date_interval_arg integer, variation_arg text, args_arg jsonb)
    returns integer language plpgsql as $$
declare
    args_id integer;
begin
    select id into args_id from cpr.clip_trade_args
    where method_id = method_id_arg and date_interval = date_interval_arg and variation = variation_arg;
    if args_id is not null then
        update cpr.clip_trade_args set args = args_arg where id = args_id;
        return args_id;
    end if;

    insert into cpr.clip_trade_args (method_id, date_interval, variation, args)
    values (method_id_arg, date_interval_arg, variation_arg, args_arg)
    on conflict (method_id, date_interval, variation) do update
    set args = excluded.args
    returning id into args_id;

    if args_id is null then
        select id into args_id from cpr.clip_trade_args
        where method_id = method_id_arg and date_interval = date_interval_arg and variation = variation_arg;
    end if;
    return args_id;
end; $$;

COMMIT;
