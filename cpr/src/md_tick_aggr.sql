create or replace function md.tick_to_candle_aggr(
    tradecode_arg text, dt_from_arg timestamptz, dt_to_arg timestamptz,
    candle_interval_arg interval default '1 minute'
) returns table (
    tradecode text, dt timestamptz,
    open float8, high float8, low float8, close float8,
    vol_open bigint, vol_close bigint,
    oi_open integer, oi_close integer
) language sql as $$
    with input as (
        select tradecode,
            dt,
            case when candle_interval_arg = interval '1 day' then
                case when extract(hour from dt) >= 19 then
                    case when extract(dow from dt) = 5 then
                        date_trunc('day', dt) + interval '3 day'
                    else
                        date_trunc('day', dt) + interval '1 day'
                    end
                else
                    date_trunc('day', dt)
                end
            else
                time_bucket(candle_interval_arg, dt)
            end as bucket_dt,
            last_price, vol, oi
        from md.contract_price_tick
        where tradecode = tradecode_arg
            and dt >= dt_from_arg
            and dt < dt_to_arg
    ), open_close as (
        select tradecode, bucket_dt,
            first_value(last_price) over (partition by bucket_dt order by case when last_price is null then 1 else 0 end asc, dt) as open_price,
            first_value(last_price) over (partition by bucket_dt order by dt desc) as close_price,
            first_value(vol) over (partition by bucket_dt order by case when oi is null then 1 else 0 end asc, dt) as vol_open,
            first_value(vol) over (partition by bucket_dt order by case when oi is null then 1 else 0 end asc, dt desc) as vol_close,
            first_value(oi) over (partition by bucket_dt order by case when oi is null then 1 else 0 end asc, dt) as oi_open,
            first_value(oi) over (partition by bucket_dt order by case when oi is null then 1 else 0 end asc, dt desc) as oi_close
        from input
    ), open_close_aggr as (
        select distinct on (tradecode, bucket_dt)
            tradecode, bucket_dt,
            open_price as open,
            close_price as close,
            vol_open, vol_close,
            oi_open, oi_close
        from open_close
    ), high_low_aggr as (
        select tradecode, bucket_dt,
            max(last_price) as high,
            min(last_price) as low
        from input
        group by tradecode, bucket_dt
    ), output as (
        select tradecode, bucket_dt as dt,
            oca.open, hla.high, hla.low, oca.close,
            oca.vol_open, oca.vol_close,
            oca.oi_open, oca.oi_close
        from open_close_aggr oca join high_low_aggr hla using (tradecode, bucket_dt)
    )
    select * from output
    order by dt asc;
$$;

create or replace function md.tick_to_minute_update_single(
        tradecode_arg text, dt_from_arg timestamptz, dt_to_arg timestamptz
) returns void language sql as $$
    insert into md.contract_price_minute(
        tradecode, dt, open, high, low, close, vol_open, vol_close, oi_open, oi_close)
    select * from md.tick_to_candle_aggr(
        tradecode_arg, dt_from_arg, dt_to_arg, interval '1 minute')
    on conflict(tradecode, dt) do update set
        open = excluded.open,
        high = excluded.high,
        low = excluded.low,
        close = excluded.close,
        vol_open = excluded.vol_open,
        vol_close = excluded.vol_close,
        oi_open = excluded.oi_open,
        oi_close = excluded.oi_close,
        updated_at = now();
$$;

create or replace function md.tick_to_minute_update_all(
    dt_from_arg timestamptz, dt_to_arg timestamptz
) returns void language plpgsql as $$
declare
    tradecode_arr text[];
begin
    select array_agg(distinct tradecode) into tradecode_arr
        from md.contract_price_tick
        where dt >= dt_from_arg and dt < dt_to_arg;
    for i in array_lower(tradecode_arr, 1)..array_upper(tradecode_arr, 1) loop
        perform md.tick_to_minute_update_single(
            tradecode_arr[i], dt_from_arg, dt_to_arg);
        raise notice 'Updated minute candle for tradecode % between % and %. (% / %)',
            tradecode_arr[i], dt_from_arg, dt_to_arg, i, array_upper(tradecode_arr, 1);
    end loop;
end;
$$;

create or replace function md.tick_to_daily_update_single(
        tradecode_arg text, dt_from_arg date, dt_to_arg date
) returns void language sql as $$
    insert into md.contract_price_daily(
        tradecode, dt, open, high, low, close, vol_open, vol_close, oi_open, oi_close)
    select * from md.tick_to_candle_aggr(
        tradecode_arg,
        dt_from_arg::timestamptz - interval '8 hour',
        dt_to_arg::timestamptz - interval '8 hour',
        interval '1 day')
    on conflict(tradecode, dt) do update set
        open = excluded.open,
        high = excluded.high,
        low = excluded.low,
        close = excluded.close,
        vol_open = excluded.vol_open,
        vol_close = excluded.vol_close,
        oi_open = excluded.oi_open,
        oi_close = excluded.oi_close,
        updated_at = now();
$$;

create or replace function md.tick_to_daily_update_all(
    dt_from_arg date, dt_to_arg date
) returns void language plpgsql as $$
declare
    tradecode_arr text[];
begin
    select array_agg(distinct tradecode) into tradecode_arr
        from md.contract_price_tick
        where dt >= dt_from_arg and dt < dt_to_arg;
    for i in array_lower(tradecode_arr, 1)..array_upper(tradecode_arr, 1) loop
        perform md.tick_to_daily_update_single(
            tradecode_arr[i], dt_from_arg, dt_to_arg);
        raise notice 'Updated daily candle for tradecode % between % and %. (% / %)',
            tradecode_arr[i], dt_from_arg, dt_to_arg, i, array_upper(tradecode_arr, 1);
    end loop;
end;
$$;




insert into md.contract_price_minute(tradecode, dt, open, high, low, close, vol_open, vol_close, oi_open, oi_close)
select * from md.tick_to_candle_aggr('159915', '2025-11-25 10:11', '2025-11-25 10:12', interval '1 minute')
on conflict(tradecode, dt) do update set
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    vol_open = excluded.vol_open,
    vol_close = excluded.vol_close,
    oi_open = excluded.oi_open,
    oi_close = excluded.oi_close,
    updated_at = now();

insert into md.contract_price_daily(tradecode, dt, open, high, low, close, vol_open, vol_close, oi_open, oi_close)
select * from md.tick_to_candle_aggr('159915', '2025-11-25 00:00', '2025-11-26 00:00', interval '1 day')
on conflict(tradecode, dt) do update set
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    vol_open = excluded.vol_open,
    vol_close = excluded.vol_close,
    oi_open = excluded.oi_open,
    oi_close = excluded.oi_close,
    updated_at = now();


