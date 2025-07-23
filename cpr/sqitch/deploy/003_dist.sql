-- Deploy cpr:003_dist to pg

BEGIN;

-- XXX Add DDLs here.
create table cpr.dt_range (
    id serial primary key,
    d1 date not null,
    d2 date not null,
    t1 time not null,
    t2 time not null
);
create unique index on cpr.dt_range (d1, d2, t1, t2);

create table cpr.method (
    id serial primary key,
    name text not null,
    variation text not null,
    args jsonb,
    description text
);
create unique index on cpr.method (name, variation);

create table cpr.clip (
    id serial primary key,
    dataset_id integer not null references cpr.dataset(id) on delete cascade,
    dt_range_id integer not null references cpr.dt_range(id) on delete cascade,
    method_id integer not null references cpr.method(id) on delete cascade,
    data jsonb not null
);
create unique index on cpr.clip (dataset_id, dt_range_id, method_id);

create function cpr.get_or_create_dt_range(d1_arg date, d2_arg date, t1_arg time, t2_arg time)
    returns integer language plpgsql
    as $$
declare
    dt_range_id integer;
begin
    insert into cpr.dt_range (d1, d2, t1, t2) values ($1, $2, $3, $4)
    on conflict do nothing returning id into dt_range_id;
    if dt_range_id is null then
        select id into dt_range_id from cpr.dt_range where d1 = $1 and d2 = $2 and t1 = $3 and t2 = $4;
    end if;
    return dt_range_id;
end; $$;

create function cpr.get_or_create_method(name_arg text, variation_arg text, args_arg jsonb, description_arg text)
    returns integer language plpgsql
    as $$
declare
    method_id integer;
begin
    insert into cpr.method (name, variation, args, description)
        values ($1, $2, $3, $4)
        on conflict(name, variation) do update set
            args = $3,
            description = $4
        returning id into method_id;
    if method_id is null then
        select id into method_id from cpr.method where name = $1 and variation = $2;
    end if;
    return method_id;
end; $$;

create function cpr.get_or_create_clip(dataset_id_arg integer, dt_range_id_arg integer, method_id_arg integer, data_arg jsonb)
    returns integer language plpgsql
    as $$
declare
    clip_id integer;
begin
    insert into cpr.clip (dataset_id, dt_range_id, method_id, data)
        values ($1, $2, $3, $4)
        on conflict(dataset_id, dt_range_id, method_id) do update set
            data = $4
        returning id into clip_id;
    if clip_id is null then
        select id into clip_id from cpr.clip where dataset_id = $1 and dt_range_id = $2 and method_id = $3;
    end if;
    return clip_id;
end; $$;

COMMIT;
