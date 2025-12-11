-- Deploy cpr:017_hb_counter to pg

BEGIN;

-- XXX Add DDLs here.
create table if not exists hb.counter (
    id serial primary key,
    username text not null,
    config_name text not null,
    counter_name text not null,
    dt timestamptz not null default now(),
    value int not null,
    updated_at timestamptz not null default now()
);

-- 不要在这个表格上定义唯一约束，这里接受并发写入，允许覆盖更新。
create index if not exists idx_hb_counter_dt_counter_username_config
    on hb.counter (dt, counter_name, username, config_name);

COMMIT;
