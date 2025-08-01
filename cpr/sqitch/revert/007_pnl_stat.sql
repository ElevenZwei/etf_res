-- Revert cpr:007_pnl_stat from pg

BEGIN;

-- XXX Add DDLs here.
drop function if exists cpr.get_best_clip_trade_args(
    bg date,
    ed date,
    dataset_id_arg int,
    cnt int
);
drop function if exists cpr.get_return_of_clip_trade_args(
    bg date,
    ed date,
    dataset_id_arg int,
    trade_args_ids int[]
);

COMMIT;
