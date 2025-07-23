-- Deploy cpr:002_cpr.sql to pg

BEGIN;

-- XXX Add DDLs here.
-- This file is used to create the cpr table in the cpr schema.
-- Ground fact table, no experimental data.

create table cpr.dataset (
    id serial primary key,
    spotcode text not null,
    expiry_priority integer not null,
    -- 1 for first expiry, 2 for second expiry, etc.
    strike numeric(12, 4) not null,
    created_at timestamptz default now()
);
create unique index on cpr.dataset (spotcode, expiry_priority, strike);

create table cpr.cpr (
    dt timestamptz not null,
    dataset_id integer not null references cpr.dataset(id) on delete cascade,
    call integer not null,
    put integer not null);
create unique index on cpr.cpr (dt, dataset_id);

select create_hypertable('cpr.cpr', 'dt',
    chunk_time_interval => interval '1 month',
    create_default_indexes => true);

create table cpr.daily (
    dt timestamptz not null,
    ti time not null,
    dataset_id integer not null references cpr.dataset(id) on delete cascade,
    ratio float8 not null,
    ratio_diff float8 not null
);
create unique index on cpr.daily (dt, dataset_id);
create index on cpr.daily (ti, dt, dataset_id);
select create_hypertable('cpr.daily', 'dt',
    chunk_time_interval => interval '1 month',
    create_default_indexes => true);

create function cpr.update_daily (dt_arg date, dataset_id_arg integer)
returns void as $$
declare
bg timestamptz := dt_arg::timestamptz;
ed timestamptz := bg + interval '1 day';
begin

    insert into cpr.daily (dt, ti, dataset_id, ratio, ratio_diff)
    with input as (
        select dt, dt::time as ti, dataset_id,
        (call - put)::float8 / greatest((call + put), 1) as ratio
        from cpr.cpr
        where dt >= bg and dt < ed and dataset_id = dataset_id_arg
    ), startpoint as (
        select dt, ti, dataset_id, ratio,
        first_value(ratio) over (partition by dataset_id order by dt) as ratio_start
        from input
    ), diff as (
        select dt, ti, dataset_id, ratio,
        ratio - ratio_start as ratio_diff
        from startpoint
    )
    select * from diff
        on conflict (dt, dataset_id) do update
        set ratio = excluded.ratio,
        ratio_diff = excluded.ratio_diff;

end $$ language plpgsql;

create function cpr.update_daily(d1 date, d2 date, dataset_id_arg integer,
    notice boolean default true)
returns void as $$
declare
    d date;
begin
    for d in select generate_series(d1, d2, interval '1 day')::date loop
        if notice then
            raise notice 'Updating daily data for %', d;
        end if;
        perform cpr.update_daily(d, dataset_id_arg);
    end loop;
end $$ language plpgsql;

COMMIT;
