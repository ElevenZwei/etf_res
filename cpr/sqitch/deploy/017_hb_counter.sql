-- Deploy cpr:017_hb_counter to pg

BEGIN;

-- XXX Add DDLs here.
create table if not exists hb.counter (
    id serial primary key,
    username text not null,
    config_name text not null,
    counter_name text not null,
    value int not null,
    updated_at timestamptz not null default now()
);

create index if not exists idx_hb_counter_username_time_config_counter
    on hb.counter (username, updated_at, config_name, counter_name);

COMMIT;
