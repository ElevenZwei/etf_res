-- 这个文件的作用是统计一段时间内的 Rolling Args 在 ETF 上面的理论收益。


begin;

-- 这里我们要注意不同的统计方式得到的收益数字有不同的含义。

-- 对多个交易的 profit_percent 直接取平均不是实际市场操作的结果。
-- 例如两笔交易的开仓价格不同，但是平仓价格一样的时候，如果我们用 profit / open_price 计算 percent。
-- 这个时候两个交易的 avg profit percent 得到的是两笔交易在不同的开仓价格投入了相同的金钱，
-- 但是不同交易数量的收益结果。

-- 实际我们操作的时候，多个策略之间的权重相同指的是投入相同的手数，而不是价格，而且价格的微小差异无法连续反映在数量上。
-- 这个角度上面的统计需要加权开仓价格平均，而不是简单平均。
-- 也就是 sum(profit) / sum(price_open) 
-- 不不不，2025-09-23 告诉我这个算出来完全不对，(a/b + c/d)/2 != (a+c)/(b+d)

-- 但是这个计算方式又引发了新的问题，如果十个策略里面只有两个策略在交易，但是我们需要准备十个策略的资金。
-- 用这两个策略的平均收益代表十个策略的平均收益也有问题，
-- 所以我们需要再加上一个交易了的策略数量的修正因子，count(distinct trade_args_id) / total_count .
-- 这样已经非常接近真实情况了。
-- 所以综合起来的正确做法是
-- sum(profit_percent) / rank_arg as profit_percent_weighted_avg,

do $$
declare
    dt_bg date = '2025-01-01';
    dt_ed date = '2025-11-01';
    -- dt_ed date = '2025-01-16';
    roll_args_id_arg int = 1;
    rank_arg int = 10;
begin
    create temp table tt as
    with roll_res as (
        select dt_from, dt_to, trade_args_id, ra.dataset_id
        from cpr.roll_result rr join cpr.roll_args ra
        on rr.roll_args_id = ra.id 
        where dt_from <= dt_ed
            and dt_to >= dt_bg
            and roll_args_id = roll_args_id_arg
            and predict_rank <= rank_arg
    ), roll_profit as (
        select *, dt_open::date as dat
        from roll_res rr join cpr.clip_trade_profit ctp
        using (trade_args_id, dataset_id)
        -- [dt_open, dt_close] \subset [dt_from, dt_to]
        -- [dt_open, dt_close] \subset [dt_bg, dt_ed]
        where true
            and rr.dt_from <= ctp.dt_open
            and rr.dt_to >= ctp.dt_close
            and ctp.dt_open >= dt_bg
            and ctp.dt_close <= dt_ed
        order by rr.dt_from, rr.dt_to, trade_args_id, ctp.dt_open
    ), roll_profit_daily as (
        select dat,
        -- sum(profit) / sum(price_open) * count(distinct trade_args_id) / rank_arg
        --     as profit_percent_weighted_avg
        sum(profit_percent) / rank_arg
            as profit_percent_weighted_avg,
        count(*) as trades_cnt,
        count(distinct trade_args_id) as active_args_cnt
        from roll_profit
        group by dat
        order by dat
    ), roll_profit_daily_accu as (
        select *, 
        sum(profit_percent_weighted_avg) over (order by dat) profit_percent_weighted_avg_accu
        from roll_profit_daily
        order by dat
    )
    -- select * from roll_profit;
    -- select * from roll_profit_daily;
    select * from roll_profit_daily_accu;
end $$;

select * from tt;
-- order by profit_percent_avg desc;

-- select * from cpr.clip_trade_profit
-- where trade_args_id = 2458
-- and dt_open >= '2025-09-08';
--
-- select * from cpr.clip_trade_profit
-- where true
-- and dt_open >= '2025-09-10'
-- order by dt_open limit 10;

-- -- Check if we have data for a time period.
-- select * from cpr.clip_trade_profit
-- where dt_open >= '2025-07-31'
-- and dt_close <= '2025-08-02'
-- order by trade_args_id
-- limit 100;

rollback;

