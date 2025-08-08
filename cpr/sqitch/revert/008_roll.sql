-- Revert cpr:008_roll from pg

BEGIN;

-- XXX Add DDLs here.

drop function if exists cpr.get_or_create_roll_args(
    dataset_id integer, roll_method_id integer,
    trade_args_from_id integer, trade_args_to_id integer, pick_count integer);
drop function if exists cpr.get_or_create_roll_method(
    name text, variation text,
    is_static boolean,
    args jsonb, description text);
drop table if exists cpr.roll_merged;
drop table if exists cpr.roll_result;
drop table if exists cpr.roll_rank;
drop table if exists cpr.roll_args;
drop table if exists cpr.roll_method;

COMMIT;
