-- Deploy cpr:009_roll_json to pg
-- 目标是储存 roll_export.py 的输出 json ，还有储存 json 和 oi csv 对应的运行结果。

BEGIN;

-- XXX Add DDLs here.

create table cpr.roll_export(
    id serial primary key,
    roll_args_id integer not null references cpr.roll_args(id) on delete cascade,
    top integer not null,
    -- dt_from 和 dt_to 是 roll_export.py 的参数，表示导出数据的时间范围。
    -- both are inclusive.
    dt_from date not null,
    dt_to date not null,
    args jsonb not null,
    created_at timestamptz not null default now(),
    check (dt_from <= dt_to),
    -- 确保给定的 roll_args_id, top 下面不会有重叠的时间范围。
    exclude using gist(daterange(dt_from, dt_to, '[]') with &&,
        roll_args_id with =, top with =)
);
create unique index if not exists cpr_roll_export_idx
    on cpr.roll_export (roll_args_id, top, dt_from, dt_to);


create table cpr.roll_export_run(
    roll_export_id integer not null references cpr.roll_export(id) on delete cascade,
    dt timestamptz not null,
    dt_raw timestamptz not null,
    position float8 not null,
    created_at timestamptz not null default now()
);
create unique index if not exists cpr_roll_export_run_idx
    on cpr.roll_export_run (roll_export_id, dt);

-- insert roll run args into cpr.roll_export
-- returns null if insert violates constraints.
create or replace function cpr.get_or_create_roll_export(
    roll_args_id_arg integer, top_arg integer, dt_from_arg date, dt_to_arg date, args_arg jsonb)
    returns integer language plpgsql as $$
declare
    export_id integer;
begin
    select id into export_id from cpr.roll_export
        where roll_args_id = roll_args_id_arg and top = top_arg
        and dt_from = dt_from_arg and dt_to = dt_to_arg;
    if export_id is not null then
        update cpr.roll_export set args = args_arg where id = export_id;
        return export_id;
    end if;
    insert into cpr.roll_export (roll_args_id, top, dt_from, dt_to, args)
        values (roll_args_id_arg, top_arg, dt_from_arg, dt_to_arg, args_arg)
        on conflict (roll_args_id, top, dt_from, dt_to) do update
            set args = excluded.args
        returning id into export_id;
    if export_id is null then
        select id into export_id from cpr.roll_export
            where roll_args_id = roll_args_id_arg and top = top_arg
            and dt_from = dt_from_arg and dt_to = dt_to_arg;
    end if;
    return export_id;
end; $$;


COMMIT;
