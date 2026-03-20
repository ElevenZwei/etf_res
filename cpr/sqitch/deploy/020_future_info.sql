-- Deploy cpr:020_future_info to pg

BEGIN;

-- XXX Add DDLs here.

-- 期货合约表格，用于记录期货合约的品类信息。
-- tradecode 是期货合约的唯一标识符，commodity 是期货合约所属的品类，例如 MA604 的 commodity 是 MA。
-- 可以通过 commodity 来查询所有相关的期货合约，例如查询所有 MA 的期货合约，或者查询所有 SC 的期货合约。
-- 以后还可以追加一些字段，例如保证金率。
create table if not exists md.future_contract (
    tradecode text primary key,
    commodity text not null,
    inserted_at timestamptz not null default now()
);
create index if not exists idx_future_contract_commodity
    on md.future_contract(commodity);

-- 期货主力合约表格，用于记录每个品类的主力合约信息。
create table if not exists md.future_main (
    tradecode text primary key,
    commodity text not null,
    valid_from date not null,
    inserted_at timestamptz not null default now()
);
create index if not exists idx_future_main_commodity_valid_from
    on md.future_main(commodity, valid_from);

create or replace view md.future_main_view as
    select distinct on (commodity) tradecode, commodity, valid_from
    from md.future_main order by commodity, valid_from desc;

create or replace view md.future_main_history as
    select commodity, tradecode, valid_from,
        lag(valid_from) over (partition by commodity order by valid_from) as valid_to
    from md.future_main order by commodity, valid_from desc;

create or replace function md.get_future_commodity(
    tradecode_arg text
) returns text language sql stable as $$
    select commodity from md.future_contract
    where tradecode = tradecode_arg;
$$;

create or replace function md.get_future_main(
    commodity_arg text, dt_arg date default current_date
) returns text language sql stable as $$
    select tradecode from md.future_main
    where commodity = commodity_arg and valid_from <= dt_arg
    order by valid_from desc limit 1;
$$;

COMMIT;
