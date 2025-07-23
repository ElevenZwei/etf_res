-- Revert cpr:001_schema from pg

BEGIN;

-- XXX Add DDLs here.
drop schema if exists cpr cascade;

COMMIT;
