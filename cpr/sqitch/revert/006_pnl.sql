-- Revert cpr:006_pnl from pg

BEGIN;

-- XXX Add DDLs here.
drop function if exists cpr.update_intraday_spot_clip_profit_range(
    dataset_id_arg integer, trade_args_id_arg integer, bg_arg date, ed_arg date) cascade;
drop function if exists cpr.update_intraday_spot_clip_profit(
    dataset_id_arg integer, trade_args_id_arg integer, dat_arg date) cascade;
drop table if exists cpr.clip_trade_profit cascade;

COMMIT;
