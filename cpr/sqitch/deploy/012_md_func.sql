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
    ('SSE', 'Shanghai Stock Exchange', 'Asia/Shanghai', false),
    ('SZSE', 'Shenzhen Stock Exchange', 'Asia/Shanghai', false)
    on conflict (exchange) do nothing;

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
    -- canonicalize
    -- 这里代为处理录制代码里面可能没有传入的 null 参数。
    if callput_arg = 0 or callput_arg is null then
        callput_arg = 0;
        spotcode_arg = null;
        chaincode_arg = null;
        strike_arg = null;
        expiry_arg = null;
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

create or replace function md.update_contract_price_daily(
    tradecode_arg text, dt_arg timestamptz,
    last_price_arg float8, vol_arg bigint, oi_arg integer)
    returns integer language plpgsql as $$
declare
    price_daily_id integer = null;
    new_open float8;
    new_high float8;
    new_low float8;
begin
    select id into price_daily_id
        from md.contract_price_daily cpd
        where cpd.tradecode = tradecode_arg and cpd.dt = dt_arg::date;
    select
        coalesce(cpd.open, last_price_arg),
        greatest(coalesce(cpd.high, last_price_arg), last_price_arg),
        least(coalesce(cpd.low, last_price_arg), last_price_arg)
        into new_open, new_high, new_low
        from md.contract_price_daily cpd
        where cpd.id = price_daily_id or price_daily_id is null;

    if price_daily_id is not null then
        update md.contract_price_daily cpd
        set
            open = new_open, high = new_high, low = new_low,
            close = last_price_arg, vol = vol_arg, oi = oi_arg,
            updated_at = now()
        where id = price_daily_id;
    else
        insert into md.contract_price_daily (
            tradecode, dt, open, high, low, close, vol, oi, days_left)
        values (
            tradecode_arg, dt_arg::date,
            last_price_arg, last_price_arg, last_price_arg, last_price_arg,
            vol_arg, oi_arg,
            (select case when ci.expiry is not null then (ci.expiry - dt_arg::date + 1)
                    else null end
                from md.contract_info ci
                where ci.tradecode = tradecode_arg)
        ) on conflict (tradecode, dt) do update set
            open = new_open, high = new_high, low = new_low,
            close = excluded.close, vol = excluded.vol, oi = excluded.oi,
            updated_at = now()
        returning id into price_daily_id;

        if price_daily_id is null then
            select id into price_daily_id
            from md.contract_price_daily cpd
            where cpd.tradecode = tradecode_arg and cpd.dt = dt_arg::date;
        end if;
    end if;
    return price_daily_id;
end;
$$;

create or replace function md.update_contract_price_minute(
    tradecode_arg text, dt_arg timestamptz,
    last_price_arg float8, vol_arg bigint, oi_arg integer)
    returns integer language plpgsql as $$
declare
    price_minute_id integer = null;
    dt_minute timestamptz = to_timestamp(floor(extract(epoch from at_arg) / 60) * 60);
    new_open float8;
    new_high float8;
    new_low float8;
begin
    select id into price_minute_id
        where tradecode = tradecode_arg and dt = dt_minute;
    select
        coalesce(cpm.open, last_price_arg),
        greatest(coalesce(cpm.high, last_price_arg), last_price_arg),
        least(coalesce(cpm.low, last_price_arg), last_price_arg)
        into new_open, new_high, new_low
        from md.contract_price_minute cpm
        where cpm.id = price_minute_id or price_minute_id is null;

    if price_minute_id is not null then
        update md.contract_price_minute cpm
        set
            open = new_open, high = new_high, low = new_low,
            close = last_price_arg, oi = oi_arg, vol = vol_arg,
            updated_at = now()
        where id = price_minute_id;
    else
        insert into md.contract_price_minute (
            tradecode, dt, open, high, low, close, vol, oi)
        values (
            tradecode_arg, dt_minute,
            last_price_arg, last_price_arg, last_price_arg, last_price_arg,
            vol_arg, oi_arg)
        on conflict (tradecode, dt) do update set
            open = new_open, high = new_high, low = new_low,
            close = excluded.close, vol = excluded.vol, oi = excluded.oi,
            updated_at = now()
        returning id into price_minute_id;

        if price_minute_id is null then
            select id into price_minute_id
            from md.contract_price_minute cpm
            where cpm.tradecode = tradecode_arg and cpm.dt = dt_minute;
        end if;
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
    select true into tradecode_exists
        from md.contract_info ci
        where ci.tradecode = tradecode_arg;
    if tradecode_exists is not true then
        raise exception 'tradecode % not exists', tradecode_arg;
        return false;
    end if;
    select md.update_contract_price_daily(
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
