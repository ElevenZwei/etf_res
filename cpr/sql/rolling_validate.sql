-- This SQL script is used to validate the best trade strategies over a specified date range.
-- It generates a series of dates based on the validation interval and calculates the average performance of the best trades.

begin;

create function cpr.best_trade_validate(
    bg date, ed date,
    validate_intv interval,
    pick_intv_ratio int,
    dataset_id_arg int,
    best_cnt int)
returns table(
    spotcode text,
    dt_bg date, dt_ed date,
    trade_args_ids text,
    dt_validate_bg date, dt_validate_ed date,
    avg_cnt text, avg_spp text, avg_spl text,
    avg_cnt_validate text,
    avg_spp_validate text, avg_spp_validate_cum text,
    avg_spl_validate text, avg_spl_validate_cum text)
language sql as $$
    with dt_series as (
        select generate_series(
            case
                when validate_intv = '1 month' then date_trunc('month', bg)
                when validate_intv = '1 week' then date_trunc('week', bg)
                else bg
            end,
            case
                when validate_intv = '1 month' then date_trunc('month', ed)
                when validate_intv = '1 week' then date_trunc('week', ed)
                else ed
            end,
            validate_intv) as dt
    ), dt_range as (
        select * from (
            select dt::date as dt_bg,
                (dt + validate_intv * pick_intv_ratio)::date as dt_ed,
                (dt + validate_intv)::date as dt_validate_ed
            from dt_series) as sub
        where dt_ed is not null
    ), dt_best_trade_args as (
        select dt_range.*, trade_args.*
        from dt_range
        cross join lateral cpr.get_best_clip_trade_args(
            dt_range.dt_bg,
            dt_range.dt_ed,
            dataset_id_arg,
            best_cnt
        ) as trade_args
    ), dt_best_agg as (
        select dt_bg, dt_ed,
            array_agg(trade_args_id) as trade_args_ids,
            avg(cnt) as avg_cnt,
            avg(spp) as avg_spp,
            avg(spl) as avg_spl
        from dt_best_trade_args
        group by dt_bg, dt_ed
    ), dt_best_validate_range as (
        select
            distinct on (d1.dt_bg)
            d1.*,
            d2.dt_bg as dt_validate_bg,
            d2.dt_validate_ed as dt_validate_ed
        from dt_best_agg d1 cross join dt_range d2
        where d1.dt_bg != d2.dt_bg
            and d1.dt_ed != d2.dt_ed
            and d1.dt_ed <= d2.dt_bg
        order by d1.dt_bg, d2.dt_bg
    ), dt_best_validate as (
        select dt_best_validate_range.*,
            val.trade_args_id as trade_arg_id,
            val.cnt as cnt_validate,
            val.spp as spp_validate,
            val.spl as spl_validate
        from dt_best_validate_range
        cross join lateral cpr.get_return_of_clip_trade_args(
            dt_best_validate_range.dt_validate_bg,
            dt_best_validate_range.dt_validate_ed,
            dataset_id_arg,
            dt_best_validate_range.trade_args_ids
        ) as val
    ), dt_best_validate_agg as (
        select dt_bg, dt_ed,
            trade_args_ids, avg_cnt, avg_spp, avg_spl,
            dt_validate_bg, dt_validate_ed,
            avg(cnt_validate) as avg_cnt_validate,
            avg(spp_validate) as avg_spp_validate,
            avg(spl_validate) as avg_spl_validate
        from dt_best_validate
        group by dt_bg, dt_ed, 
            trade_args_ids, avg_cnt, avg_spp, avg_spl,
            dt_validate_bg, dt_validate_ed
        order by dt_bg, dt_ed, dt_validate_bg, dt_validate_ed
    ), dt_best_validate_agg_sum as (
        (select *,
            sum(avg_spp_validate) over (order by dt_validate_bg) as avg_spp_validate_cum,
            exp(sum(ln(avg_spl_validate + 1)) over (order by dt_validate_bg)) - 1 as avg_spl_validate_cum
            from dt_best_validate_agg)
        union
        (select
            null as dt_bg, null as dt_ed,
            null as trade_args_ids,
            sum(avg_cnt) as avg_cnt,
            sum(avg_spp) as avg_spp,
            sum(avg_spl) as avg_spl,
            null as dt_validate_bg, null as dt_validate_ed,
            sum(avg_cnt_validate) as avg_cnt_validate,
            sum(avg_spp_validate) as avg_spp_validate,
            exp(sum(ln(avg_spl_validate + 1))) - 1 as avg_spl_validate,
            null, null
        from dt_best_validate_agg)
    ), dt_best_validate_agg_sum_tag as (
        with spotcode as (
            select spotcode from cpr.dataset where id = dataset_id_arg
        )
        select spotcode.spotcode, dt_bg, dt_ed,
            case when trade_args_ids is null then 'sum up' else array_to_string(trade_args_ids, ',') end
                as trade_args_ids_tag,
            dt_validate_bg, dt_validate_ed,
            to_char(avg_cnt / pick_intv_ratio, 'FM9999.0') as avg_cnt,
            to_char(avg_spp / pick_intv_ratio, 'FM99.0000') as avg_spp,
            to_char(avg_spl / pick_intv_ratio, 'FM99.0000') as avg_spl,
            to_char(avg_cnt_validate, 'FM9999.0') as avg_cnt_validate,
            to_char(avg_spp_validate, 'FM99.0000') as avg_spp_validate,
            to_char(avg_spp_validate_cum, 'FM99.0000') as avg_spp_validate_cum,
            to_char(avg_spl_validate, 'FM99.0000') as avg_spl_validate,
            to_char(avg_spl_validate_cum, 'FM99.0000') as avg_spl_validate_cum
        from dt_best_validate_agg_sum cross join spotcode
        order by dt_bg nulls last
    )
    select * from dt_best_validate_agg_sum_tag;
$$;

select * from cpr.best_trade_validate(
    '2025-01-01',
    '2025-07-01',
    '1 month',
    1,
    3,
    10);

select * from cpr.best_trade_validate(
    '2025-01-01',
    '2025-07-01',
    '1 week',
    4,
    3,
    10);

select * from cpr.best_trade_validate(
    '2025-01-01',
    '2025-07-01',
    '1 week',
    1,
    3,
    10);

select * from cpr.best_trade_validate(
    '2025-01-01',
    '2025-07-01',
    '1 month',
    1,
    4,
    10);

select * from cpr.best_trade_validate(
    '2025-01-01',
    '2025-07-01',
    '1 week',
    4,
    4,
    10);

select * from cpr.best_trade_validate(
    '2025-01-01',
    '2025-07-01',
    '1 week',
    1,
    4,
    10);

rollback;
