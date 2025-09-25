-- Deploy cpr:007_pnl_stat to pg

BEGIN;

-- XXX Add DDLs here.

-- 统计各个 trade_args_id 在某个时间段内的 pnl 表现。
-- 这里的 pnl 计算是基于 clip_trade_profit 表的。
-- 也就是说，必须先运行 update_intraday_spot_clip_profit 函数，生成 clip_trade_profit 表的数据。
-- 时间范围是 左闭右开 [bg, ed)。
-- ed is exclusive.
create or replace function cpr.get_best_clip_trade_args(
    bg date,
    ed date,
    dataset_id_arg int,
    cnt int default 10
) returns table(trade_args_id int, cnt int, spp numeric, spl numeric)
language sql as $$
    select trade_args_id,
        count(*) as cnt,
        sum(profit_percent) as spp,
        exp(sum(profit_logret)) - 1 as spl
    from cpr.clip_trade_profit
    where dt_open >= bg and dt_open < ed
        and dataset_id = dataset_id_arg
        and trade_args_id <= 8082
    group by trade_args_id
    order by spl desc, trade_args_id asc
    limit cnt;
$$;

-- 统计各个 trade_args_id 在某个时间段内的 pnl 表现。
-- 这里的 pnl 计算是基于 clip_trade_profit 表的。
-- 时间范围是 左闭右开 [bg, ed)。
-- ed is exclusive.
create or replace function cpr.get_return_of_clip_trade_args(
    bg date,
    ed date,
    dataset_id_arg int,
    trade_args_ids int[]
) returns table(trade_args_id int, cnt int, spp numeric, spl numeric)
language sql as $$
    select t.id as trade_args_id,
        count(profit_percent) as cnt,
        coalesce(sum(profit_percent), 0) as spp,
        coalesce(exp(sum(profit_logret)) - 1, 0) as spl
    from unnest(trade_args_ids) as t(id)
    left join cpr.clip_trade_profit ctp
        on dt_open >= bg and dt_open < ed
        and dataset_id = dataset_id_arg
        and trade_args_id = t.id
    group by t.id;
$$;

COMMIT;
