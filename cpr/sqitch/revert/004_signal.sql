-- Revert cpr:004_signal from pg

BEGIN;

-- XXX Add DDLs here.
drop function if exists cpr.get_or_create_clip_trade_args(
    method_id_arg integer, date_interval_arg integer, variation_arg text, args_arg jsonb);
drop table if exists cpr.clip_trade_backtest;
drop table if exists cpr.clip_trade_args;

COMMIT;
