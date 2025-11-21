-- Revert cpr:017_hb_counter from pg

BEGIN;

-- XXX Add DDLs here.
drop table if exists hb.counter cascade;

COMMIT;
