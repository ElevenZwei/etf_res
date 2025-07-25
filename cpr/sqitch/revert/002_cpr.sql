-- Revert cpr:002_cpr from pg

BEGIN;

-- XXX Add DDLs here.
drop function if exists cpr.update_daily (d1 date, d2 date, dataset_id integer) cascade;
drop function if exists cpr.update_daily (dt date, dataset_id integer) cascade;
drop table if exists cpr.cpr cascade;
drop table if exists cpr.daily cascade;
drop table if exists cpr.dataset cascade;

COMMIT;
