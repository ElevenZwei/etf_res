-- Revert cpr:011_md from pg

BEGIN;

-- XXX Add DDLs here.
drop schema if exists md cascade;

COMMIT;
