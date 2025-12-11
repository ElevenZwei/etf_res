-- Deploy cpr:010_persistence to pg

BEGIN;

-- XXX Add DDLs here.
create schema if not exists hb;

-- postgres need many grants to use the hb schema.
grant usage, create on schema hb to option;
grant select, insert, update, delete on all tables in schema hb to option;
grant usage, update on all sequences in schema hb to option;
grant execute on all functions in schema hb to option;

alter default privileges in schema hb
    grant select, insert, update, delete on tables to option;
alter default privileges in schema hb
    grant usage, update on sequences to option;
alter default privileges in schema hb
    grant execute on functions to option;

alter role option set search_path to hb, cpr, public;

-- Spirit persistence is used to store key-value pairs for each user and each spirit.
create table if not exists hb.spirit_persistence (
    id serial primary key,
    username text not null,
    config_name text not null,
    spirit text not null,
    key text not null,
    value jsonb,
    updated_at timestamptz not null default now()
);
create unique index if not exists hb_spirit_persistence_idx
    on hb.spirit_persistence (username, config_name, spirit, key);

create table if not exists hb.spirit_persistence_history (
    id serial primary key,
    username text not null,
    config_name text not null,
    spirit text not null,
    key text not null,
    value jsonb,
    updated_at timestamptz not null default now()
);

-- overload function without set_spirit_persistence, set it to null, for backward compatibility
create or replace function hb.set_spirit_persistence(
    username_arg text,
    spirit_arg text, key_arg text, value_arg jsonb)
    returns void language plpgsql as $$
begin
    perform hb.set_spirit_persistence(
        username_arg, null, spirit_arg, key_arg, value_arg);
end;
$$;

create or replace function hb.set_spirit_persistence(
    username_arg text, config_name_arg text,
    spirit_arg text, key_arg text, value_arg jsonb)
    returns void language plpgsql as $$
declare
    sp_id integer;
begin
    -- update history first
    insert into hb.spirit_persistence_history (username, config_name, spirit, key, value)
        values (username_arg, coalesce(config_name_arg, ''),
                spirit_arg, key_arg, value_arg);
    -- update or insert main table
    select id into sp_id from hb.spirit_persistence
        where username = username_arg
        and (config_name_arg is null or config_name = config_name_arg)
        and spirit = spirit_arg and key = key_arg
        order by updated_at desc
        limit 1;
    if sp_id is not null then
        update hb.spirit_persistence
            set value = value_arg, updated_at = now()
            where id = sp_id;
        return;
    end if;
    insert into hb.spirit_persistence (username, config_name, spirit, key, value)
        values (username_arg, coalesce(config_name_arg, ''),
                spirit_arg, key_arg, value_arg)
        on conflict (username, config_name, spirit, key) do update
            set value = excluded.value, updated_at = now();
end;
$$;

create or replace function hb.list_user_persistence(username_arg text)
    returns table(spirit text, key text, value jsonb, updated_at timestamptz)
    language sql as $$
    select spirit, key, value, updated_at from hb.spirit_persistence
        where username = username_arg
        order by config_name, spirit, key;
$$;

create or replace function hb.list_user_persistence(
    username_arg text, config_name_arg text)
    returns table(spirit text, key text, value jsonb, updated_at timestamptz) language sql as $$
    select spirit, key, value, updated_at from hb.spirit_persistence
        where username = username_arg and config_name = config_name_arg
        order by spirit, key;
$$;

create or replace function hb.clear_user_persistence(username_arg text)
    returns void language sql as $$
    delete from hb.spirit_persistence
        where username = username_arg;
$$;

create or replace function hb.clear_user_persistence(
    username_arg text, config_name_arg text)
    returns void language sql as $$
    delete from hb.spirit_persistence
        where username = username_arg
        and config_name = config_name_arg;
$$;

-- Spirit position is used to store the position of each user and each spirit and each security trade code.
create table if not exists hb.spirit_position (
    id serial primary key,
    username text not null,
    config_name text not null,
    spirit text not null,
    code text not null,
    position jsonb not null,
    updated_at timestamptz not null default now()
);

create unique index if not exists hb_spirit_position_idx
    on hb.spirit_position (username, config_name, spirit, code);

create table if not exists hb.spirit_position_history (
    id serial primary key,
    username text not null,
    config_name text not null,
    spirit text not null,
    code text not null,
    position jsonb not null,
    updated_at timestamptz not null default now()
);

-- overload function without config_name_arg, set it to null, for backward compatibility
create or replace function hb.set_spirit_position(
    username_arg text,
    spirit_arg text, code_arg text, position_arg jsonb)
    returns void language plpgsql as $$
begin
    perform hb.set_spirit_position(
        username_arg, null, spirit_arg, code_arg, position_arg);
end;
$$;

create or replace function hb.set_spirit_position(
    username_arg text, config_name_arg text,
    spirit_arg text, code_arg text, position_arg jsonb)
    returns void language plpgsql as $$
declare
    sp_id integer;
begin
    -- update history first
    insert into hb.spirit_position_history (username, config_name, spirit, code, position)
        values (username_arg, coalesce(config_name_arg, ''),
                spirit_arg, code_arg, position_arg);
    -- update or insert main table
    select id into sp_id from hb.spirit_position
        where username = username_arg
        and (config_name_arg is null or config_name = config_name_arg)
        and spirit = spirit_arg and code = code_arg
        order by updated_at desc
        limit 1;
    if sp_id is not null then
        update hb.spirit_position
            set position = position_arg, updated_at = now()
            where id = sp_id;
        return;
    end if;
    insert into hb.spirit_position (username, config_name, spirit, code, position)
        values (username_arg, coalesce(config_name_arg, ''),
            spirit_arg, code_arg, position_arg)
        on conflict (username, config_name, spirit, code) do update
            set position = excluded.position, updated_at = now();
end;
$$;

create or replace function hb.list_user_position(username_arg text)
    returns table(spirit text, code text, pos jsonb, updated_at timestamptz)
    language sql as $$
    select spirit, code, position, updated_at from hb.spirit_position
        where username = username_arg
        order by spirit, code;
$$;

create or replace function hb.list_user_position(
    username_arg text, config_name_arg text)
    returns table(spirit text, code text, pos jsonb, updated_at timestamptz)
    language sql as $$
    select spirit, code, position, updated_at from hb.spirit_position
        where username = username_arg and config_name = config_name_arg
        order by spirit, code;
$$;

create or replace function hb.clear_user_position(username_arg text)
    returns void language sql as $$
    delete from hb.spirit_position
        where username = username_arg;
$$;

create or replace function hb.clear_user_position(
    username_arg text, config_name_arg text)
    returns void language sql as $$
    delete from hb.spirit_position
        where username = username_arg and config_name = config_name_arg;
$$;

COMMIT;

