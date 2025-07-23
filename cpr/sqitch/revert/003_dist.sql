-- Revert cpr:003_dist from pg

BEGIN;

-- XXX Add DDLs here.
drop function if exists cpr.get_or_create_dt_range(d1_arg date, d2_arg date, t1_arg time, t2_arg time) cascade;
drop function if exists cpr.get_or_create_method(name_arg text, variation_arg text, args_arg jsonb, description_arg text) cascade;
drop function if exists cpr.get_or_create_clip(dataset_id_arg integer, dt_range_id_arg integer, method_id_arg integer, data_arg jsonb) cascade;

drop table if exists cpr.clip cascade;
drop table if exists cpr.method cascade;
drop table if exists cpr.dt_range cascade;

COMMIT;
