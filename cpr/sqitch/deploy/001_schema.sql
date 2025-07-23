-- Deploy cpr:001_schema to pg

BEGIN;

-- XXX Add DDLs here.

create or replace function assert(condition boolean, message text)
returns void as $$
begin
    if not condition then
        raise exception 'assertion failed: %', message;
    end if;
end;
$$ language plpgsql;

create schema if not exists cpr;

-- Create the option role if it does not exist.
-- replace the password with a secure one.
create role if not exists option with login
    password 'option'
    valid until 'infinity';

-- replace the database name with the actual database name.
grant connect on database opt to option;

-- postgres need many grants to use the cpr schema.
grant usage on schema cpr to option;
grant select, insert, update, delete on all tables in schema cpr to option;
grant usage, update on all sequences in schema cpr to option;
grant execute on all functions in schema cpr to option;

alter default privileges in schema cpr
    grant select, insert, update, delete on tables to option;
alter default privileges in schema cpr
    grant usage, update on sequences to option;
alter default privileges in schema cpr
    grant execute on functions to option;

alter role option set search_path to cpr, public;

COMMIT;
