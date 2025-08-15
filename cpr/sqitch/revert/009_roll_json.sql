-- Revert cpr:009_roll_json from pg

BEGIN;

-- XXX Add DDLs here.
drop function if exists cpr.get_or_create_roll_export(
    roll_args_id_arg integer, top_arg integer, dt_from_arg date, dt_to_arg date, args_arg jsonb);
drop table if exists cpr.roll_export;
drop table if exists cpr.roll_export_run;

COMMIT;
