-- Deploy cpr:006_pnl to pg

BEGIN;

-- XXX Add DDLs here.

create table if not exists cpr.clip_trade_profit(
    dataset_id integer not null references cpr.dataset(id) on delete cascade,
    trade_args_id integer not null references cpr.clip_trade_args(id) on delete cascade,
    dt_open timestamptz not null,
    dt_close timestamptz not null,
    price_open numeric(12, 4) not null,
    price_close numeric(12, 4) not null,
    amount integer not null, -- positive for long, negative for short
    profit numeric(12, 4) not null,
    profit_percent float8 not null,
    profit_logret float8 not null
);
create unique index if not exists cpr_clip_trade_profit_idx
    on cpr.clip_trade_profit (dataset_id, trade_args_id, dt_open);
create index if not exists cpr_clip_trade_profit_trade_args_idx
    on cpr.clip_trade_profit (trade_args_id, dt_open);

-- 这里要写一个通过 clip_trade_backtest 和 market_minute 价格计算出当日 PNL 的函数。
-- 原理是用 clip_trade_backtest.position 字段的每个 change 作为买卖时间点，计算每一笔交易的得失。

create or replace function cpr.update_intraday_spot_clip_profit (
    dataset_id_arg integer, trade_args_id_arg integer, dat_arg date)
    returns void language plpgsql as $$
begin
    insert into cpr.clip_trade_profit (
        dataset_id, trade_args_id, dt_open, dt_close,
        price_open, price_close,
        amount, profit, profit_percent, profit_logret)

    with input as (
        select dt, dataset_id, trade_args_id, position,
        lag(position) over (partition by dataset_id, trade_args_id order by dt) as position_prev
        from cpr.clip_trade_backtest
        where dataset_id = dataset_id_arg and trade_args_id = trade_args_id_arg
        and dt > dat_arg::timestamptz and dt < (dat_arg + interval '1 day')::timestamptz
    ), change as (
        select dt as dt1,
            lead(dt) over (partition by dataset_id, trade_args_id order by dt) as dt2,
            dataset_id, trade_args_id, position, position_prev,
            position - position_prev as position_change
        from input
        where position != position_prev
    ), trades as (
        -- 在这一步假设了所有仓位都是一次开满，一次平完。
        -- 因为 position_prev = 0 只保留从空仓的开仓，
        -- 同时 dt2 只保留开仓之后下一个交易的时间点，
        -- 把它当作平仓时间点。
        select dt1 as dt_open, dt2 as dt_close,
            dataset_id, trade_args_id,
            position_change as amount
        from change
        where position_prev = 0
    ), spotcode as (
        select spotcode from cpr.dataset where id = dataset_id_arg
    ), prices as (
        select t.dt_open, t.dt_close,
            t.dataset_id, t.trade_args_id, amount,
            m1.closep as price_open, m2.closep as price_close
        from trades t cross join spotcode
        join cpr.market_minute m1
            on m1.dt = t.dt_open and m1.code = spotcode.spotcode
        join cpr.market_minute m2
            on m2.dt = t.dt_close and m2.code = spotcode.spotcode
    ), profits as (
        select dt_open, dt_close, dataset_id, trade_args_id,
            price_open, price_close, amount,
            (price_close - price_open) * amount as profit
        from prices
    ), profits1 as (
        select dt_open, dt_close, dataset_id, trade_args_id,
            price_open, price_close, amount,
            profit,
            profit / greatest(price_open, 0.0001) / amount as profit_percent
            from profits
    ), profit2 as (
        select
            dataset_id, trade_args_id,
            dt_open, dt_close,
            price_open, price_close, amount,
            profit, profit_percent,
            log(greatest(profit_percent + 1, 0.0001)) as profit_logret
        from profits1
    )
    select * from profit2
        on conflict (dataset_id, trade_args_id, dt_open) do update
        set dt_close = excluded.dt_close,
            price_open = excluded.price_open,
            price_close = excluded.price_close,
            amount = excluded.amount,
            profit = excluded.profit,
            profit_percent = excluded.profit_percent,
            profit_logret = excluded.profit_logret;

end; $$;

create or replace function cpr.update_intraday_spot_clip_profit_range(
    dataset_id_arg integer,
    trade_args_id_bg_arg integer,
    trade_args_id_ed_arg integer,
    date_bg_arg date, date_ed_arg date,
    notice boolean default true)
    returns void language plpgsql as $$
declare
    dat date;
    trade_args_id integer;
begin
    for trade_args_id in select id from cpr.clip_trade_args
        where id >= trade_args_id_bg_arg and id <= trade_args_id_ed_arg
        order by id asc
    loop
        if notice then
            raise notice 'Updating profit for dataset_id: %, trade_args_id: %', dataset_id_arg, trade_args_id;
        end if;
        for dat in select generate_series(date_bg_arg, date_ed_arg, interval '1 day')::date loop
            perform cpr.update_intraday_spot_clip_profit(dataset_id_arg, trade_args_id, dat);
        end loop;
    end loop;
end; $$;

COMMIT;
