-- Revert cpr:003_dist from pg

BEGIN;

-- XXX Add DDLs here.
drop table if exists cpr.dist cascade;

COMMIT;
