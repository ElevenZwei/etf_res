-- Deploy cpr:012_md_func to pg

BEGIN;

-- XXX Add DDLs here.

insert into md.exchange_info (exchange, name, timezone, is_commodity)
    values
    ('CFFEX', 'China Financial Futures Exchange', 'Asia/Shanghai', true),
    ('SHFE', 'Shanghai Futures Exchange', 'Asia/Shanghai', true),
    ('DCE', 'Dalian Commodity Exchange', 'Asia/Shanghai', true),
    ('CZCE', 'Zhengzhou Commodity Exchange', 'Asia/Shanghai', true),
    ('INE', 'Shanghai International Energy Exchange', 'Asia/Shanghai', true),
    ('GFEX', 'Guangzhou Futures Exchange', 'Asia/Shanghai', true),
    ('SSE', 'Shanghai Stock Exchange', 'Asia/Shanghai', false),
    ('SZSE', 'Shenzhen Stock Exchange', 'Asia/Shanghai', false)
    ('BSE', 'Beijing Stock Exchange', 'Asia/Shanghai', false)
    on conflict (exchange) do nothing;

insert into md.contract_info (tradecode, name, exchange, lot_size, callput) values
    ('159915', '159915', 'SZSE', 10000, 0),
    ('510050', '510050', 'SSE', 10000, 0),
    ('510300', '510300', 'SSE', 10000, 0),
    ('510500', '510500', 'SSE', 10000, 0),
    ('588000', '588000', 'SSE', 10000, 0)
    on conflict (tradecode) do nothing;

do $$
declare
    inserted bool;
begin
    select tradecode is not null into inserted
        from md.contract_info_history
        where tradecode = '159915';
    if not inserted then
        insert into md.contract_info_history (tradecode, name, exchange, lot_size, callput) values
            ('159915', '159915', 'SZSE', 10000, 0),
            ('510050', '510050', 'SSE', 10000, 0),
            ('510300', '510300', 'SSE', 10000, 0),
            ('510500', '510500', 'SSE', 10000, 0),
            ('588000', '588000', 'SSE', 10000, 0);
    end if;
end;
$$;

create or replace function md.get_contract_of_date(
    tradecode_arg text, dt_arg date)
    returns table (
        tradecode text,
        name text,
        exchange text,
        spotcode text,
        lot_size integer,
        chaincode text,
        strike float8,
        callput integer,
        expiry date,
        updated_at timestamptz
    ) language sql as $$
    select ci.tradecode, ci.name, ci.exchange, ci.spotcode,
        ci.lot_size, ci.chaincode, ci.strike, ci.callput, ci.expiry,
        ci.updated_at
    from md.contract_info_history ci
    where ci.tradecode = tradecode_arg
        and ci.updated_at <= (dt_arg + interval '1 day')
    order by ci.updated_at desc
    limit 1;
$$;

create or replace function md.list_contracts_of_date(
    dt_arg date)
    returns table (
        tradecode text,
        name text,
        exchange text,
        spotcode text,
        lot_size integer,
        chaincode text,
        strike float8,
        callput integer,
        expiry date,
        updated_at timestamptz
) language sql as $$
    select distinct on (ci.tradecode)
        ci.tradecode, ci.name, ci.exchange, ci.spotcode,
        ci.lot_size, ci.chaincode, ci.strike, ci.callput, ci.expiry,
        ci.updated_at
    from md.contract_info_history ci
    where ci.updated_at <= (dt_arg + interval '1 day')
    order by ci.tradecode, ci.updated_at desc;
$$;

create or replace function md.upsert_contract_info(
    tradecode_arg text,
    name_arg text,
    exchange_arg text,
    spotcode_arg text,
    lot_size_arg integer,
    chaincode_arg text,
    strike_arg float8,
    callput_arg integer,
    expiry_arg date)
    returns void language plpgsql as $$
declare
    is_diff boolean = null;
begin
    -- sanitize inputs.
    -- 这里代为处理录制代码里面可能没有传入的 null 参数。
    if callput_arg = 0 or callput_arg is null then
        callput_arg = 0;
        spotcode_arg = null;
        chaincode_arg = null;
        strike_arg = null;
    end if;
    if length(spotcode_arg) = 0 then
        spotcode_arg = null;
    end if;
    if length(chaincode_arg) = 0 then
        chaincode_arg = null;
    end if;

    -- check if has differences.
    select (ci.name is distinct from name_arg)
        or (ci.exchange is distinct from exchange_arg)
        or (ci.spotcode is distinct from spotcode_arg)
        or (ci.lot_size is distinct from lot_size_arg)
        or (ci.chaincode is distinct from chaincode_arg)
        or (ci.strike is distinct from strike_arg)
        or (ci.callput is distinct from callput_arg)
        or (ci.expiry is distinct from expiry_arg)
        into is_diff
        from md.contract_info ci
        where ci.tradecode = tradecode_arg;

    if is_diff is false then
        -- no change
        return;
    end if;

    insert into md.contract_info_history (
        tradecode, name, exchange, spotcode,
        lot_size, chaincode, strike, callput, expiry)
    values (
        tradecode_arg, name_arg, exchange_arg, spotcode_arg,
        lot_size_arg, chaincode_arg, strike_arg, callput_arg, expiry_arg);

    if is_diff is true then
        update md.contract_info
        set name = name_arg,
            exchange = exchange_arg,
            spotcode = spotcode_arg,
            lot_size = lot_size_arg,
            chaincode = chaincode_arg,
            strike = strike_arg,
            callput = callput_arg,
            expiry = expiry_arg,
            updated_at = now()
        where tradecode = tradecode_arg;
        return;
    end if;

    insert into md.contract_info (
        tradecode, name, exchange, spotcode,
        lot_size, chaincode, strike, callput, expiry)
    values (
        tradecode_arg, name_arg, exchange_arg, spotcode_arg,
        lot_size_arg, chaincode_arg, strike_arg, callput_arg, expiry_arg);
end;
$$;

-- last_price_arg / vol_arg / oi_arg is nullable.
create or replace function md.update_contract_price_daily1(
    tradecode_arg text, dt_arg date,
    last_price_arg float8, vol_arg bigint, oi_arg integer)
    returns integer language plpgsql as $$
declare
    price_daily_id integer = null;
begin
    select id into price_daily_id
        from md.contract_price_daily cpd
        where cpd.tradecode = tradecode_arg and cpd.dt = dt_arg;

    if price_daily_id is not null then
        update md.contract_price_daily cpd
        set
            open = coalesce(cpd.open, last_price_arg),
            high = greatest(coalesce(cpd.high, last_price_arg), last_price_arg),
            low = least(coalesce(cpd.low, last_price_arg), last_price_arg),
            close = last_price_arg,
            vol_open = coalesce(cpd.vol_open, vol_arg),
            vol_close = vol_arg,
            oi_open = coalesce(cpd.oi_open, oi_arg),
            oi_close = oi_arg,
            updated_at = now()
        where id = price_daily_id;
        return price_daily_id;
    end if;

    insert into md.contract_price_daily (
        tradecode, dt,
        open, high, low, close,
        vol_open, vol_close, oi_open, oi_close, days_left)
    values (
        tradecode_arg, dt_arg,
        last_price_arg, last_price_arg, last_price_arg, last_price_arg,
        vol_arg, vol_arg, oi_arg, oi_arg,
        (select case when ci.expiry is not null then (ci.expiry - dt_arg + 1)
                else null end
            from md.contract_info ci
            where ci.tradecode = tradecode_arg)
    ) on conflict (tradecode, dt) do nothing
    returning id into price_daily_id;

    if price_daily_id is null then
        select md.update_contract_price_daily1(
            tradecode_arg, dt_arg, last_price_arg, vol_arg, oi_arg)
        into price_daily_id;
    end if;

    return price_daily_id;
end;
$$;

-- last_price_arg / vol_arg / oi_arg is nullable.
create or replace function md.update_contract_price_daily2(
    tradecode_arg text, dt_arg timestamptz,
    last_price_arg float8, vol_arg bigint, oi_arg integer)
    returns integer language plpgsql as $$
declare
    dt_date date = dt_arg::date;
begin
    if extract(hour from dt_arg) >= 19 then
        -- move dt_date to next trade day
        if extract(dow from dt_arg) >= 5 then
            dt_date = dt_date + (8 - extract(dow from dt_arg))::int;
        else
            dt_date = dt_date + 1;
        end if;
    end if;
    return md.update_contract_price_daily1(
        tradecode_arg, dt_date, last_price_arg, vol_arg, oi_arg);
end;
$$;

-- last_price_arg is nullable.
create or replace function md.update_contract_price_minute(
    tradecode_arg text, dt_arg timestamptz,
    last_price_arg float8, vol_arg bigint, oi_arg integer)
    returns integer language plpgsql as $$
declare
    price_minute_id integer = null;
    dt_minute timestamptz = to_timestamp(floor(extract(epoch from dt_arg) / 60) * 60);
begin
    select id into price_minute_id
        from md.contract_price_minute cpm
        where tradecode = tradecode_arg and dt = dt_minute;

    if price_minute_id is not null then
        update md.contract_price_minute cpm
        set
            open = coalesce(cpm.open, last_price_arg),
            high = greatest(coalesce(cpm.high, last_price_arg), last_price_arg),
            low = least(coalesce(cpm.low, last_price_arg), last_price_arg),
            close = last_price_arg,
            vol_open = coalesce(cpm.vol_open, vol_arg),
            vol_close = vol_arg,
            oi_open = coalesce(cpm.oi_open, oi_arg),
            oi_close = oi_arg,
            updated_at = now()
        where id = price_minute_id;
        return price_minute_id;
    end if;

    insert into md.contract_price_minute (
        tradecode, dt,
        open, high, low, close,
        vol_open, vol_close, oi_open, oi_close)
    values (
        tradecode_arg, dt_minute,
        last_price_arg, last_price_arg, last_price_arg, last_price_arg,
        vol_arg, vol_arg, oi_arg, oi_arg)
    on conflict (tradecode, dt) do nothing
    returning id into price_minute_id;

    if price_minute_id is null then
        select md.update_contract_price_minute(
            tradecode_arg, dt_arg, last_price_arg, vol_arg, oi_arg)
        into price_minute_id;
    end if;

    return price_minute_id;
end;
$$;

create or replace function md.update_contract_price(
    tradecode_arg text, dt_arg timestamptz,
    last_price_arg float8, vol_arg bigint, oi_arg integer)
    returns boolean language plpgsql as $$
declare
    tradecode_exists boolean = null;
    price_daily_id integer = null;
    price_minute_id integer = null;
begin
    -- sanitize inputs.
    if last_price_arg is not distinct from 'NaN'::numeric then
        last_price_arg = null;
    end if;
    if vol_arg <= 0 then
        vol_arg = 0;
    end if;
    if oi_arg <= 0 then
        oi_arg = 0;
    end if;
    select true into tradecode_exists
        from md.contract_info ci
        where ci.tradecode = tradecode_arg;
    if tradecode_exists is not true then
        raise exception 'tradecode % not exists', tradecode_arg;
        return false;
    end if;
    select md.update_contract_price_daily2(
        tradecode_arg, dt_arg, last_price_arg, vol_arg, oi_arg)
        into price_daily_id;
    if price_daily_id is null then
        raise exception 'failed to update daily price for % at %', tradecode_arg, dt_arg;
        return false;
    end if;
    select md.update_contract_price_minute(
        tradecode_arg, dt_arg, last_price_arg, vol_arg, oi_arg)
        into price_minute_id;
    if price_minute_id is null then
        raise exception 'failed to update minute price for % at %', tradecode_arg, dt_arg;
        return false;
    end if;
    return true;
end;
$$;


COMMIT;
