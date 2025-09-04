-- Revert cpr:010_persistence from pg

BEGIN;

-- XXX Add DDLs here.
drop schema if exists hb cascade;

COMMIT;
