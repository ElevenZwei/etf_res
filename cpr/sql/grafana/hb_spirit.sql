-- Description: Grafana queries for HB spirit data

-- Panel: spirit position BuyOne, zero fill from previous position if empty
with input as (
    select username, spirit, code, updated_at, position,
    lag(position) over (partition by username, spirit, code order by updated_at) as prev_position
    from "hb"."spirit_position_history"
    where username = '$Username'
        and spirit = 'BuyOne'
        and $__timeFilter(updated_at)
)
, prev_pos_unpack as (
    select username, spirit, code, updated_at, position, prev_item
    from input
    -- left join lateral unnest of jsonb array, prevent empty prev_position causing row drop
    left join lateral (
        select jsonb_array_elements(coalesce(prev_position, '[]'::jsonb)) as prev_item
    ) as lp on true
)
, prev_pos_zeroed as (
    select username, spirit, code, updated_at, position,
    jsonb_set(prev_item, '{amount}', '0'::jsonb) as prev_item_zeroed
    from prev_pos_unpack
)
, zeroed_pack as (
    select username, spirit, code, updated_at, position,
    coalesce(
        jsonb_agg(prev_item_zeroed) filter (where prev_item_zeroed is not null),
        '[]'::jsonb) as prev_position_zeroed
    from prev_pos_zeroed
    group by username, spirit, code, updated_at, position
)
, spirit_position as (
    select username, spirit, code, updated_at, position,
    case when position is null or jsonb_array_length(position) = 0 then prev_position_zeroed
    else position end as position_filled
    from zeroed_pack
)
, position_unpack as (
    select username, spirit, code, updated_at, item
    from spirit_position
    left join lateral (
        select jsonb_array_elements(position_filled) as item
    ) as lp on true
)
, position_final as (
    select username, spirit, code, updated_at,
    coalesce((item->>'contract'), '<empty>') as contract,
    coalesce((item->'amount')::int, 0) as amount
    from position_unpack
)
, position_aggregated as (
    select updated_at as time,
    username || '@' || spirit || '@' || contract as tag,
    sum(amount) as amount
    from position_final
    group by updated_at, username, spirit, contract
    order by updated_at asc
)
select * from position_aggregated
order by time;

-- Panel: spirit position all spirits, zero fill from previous position if empty
-- Assume position jsonb array elements with same contract should be summed up
-- e.g. [{"contract": "AAPL", "amount": 10}, {"contract": "AAPL", "amount": 5}] => 15
-- Result tag format: username@contract
-- 注意这个做法并不正确，因为不同的 spirit 更新的时间并不同步，这里我们需要延续其他 spirit
-- 之前的持仓，实际上是非常复杂的逻辑，所以我放在 spirit catcher C++ 里面操作了。
-- 这里的查询仅作为参考。
with input as (
    select username, code, updated_at, position
    from "hb"."spirit_position_history"
    where username = '$Username'
        and spirit not like 'SpiritCatcher%'
        and $__timeFilter(updated_at)
)
, input_unpack as (
    select username, code, updated_at,
        item->>'contract' as contract,
        (item->'amount')::int as amount
    from input
    left join lateral (
        select jsonb_array_elements(position) as item
    ) as lp on true
)
, input_sum as (
    select username, code, updated_at,
    contract, sum(amount) as amount
    from input_unpack
    group by username, code, updated_at, contract
)
, input_item as (
    select username, code, updated_at, contract,
        jsonb_set(
            jsonb_set(
                '{}'::jsonb, '{contract}', ('"' || contract || '"')::jsonb),
            '{amount}', (sum(amount)::text)::jsonb) as item
    from input_sum
    group by username, code, updated_at, contract
)
, input_repack as (
    select username, code, updated_at,
        coalesce(
            jsonb_agg(item) filter (where item is not null),
            '[]'::jsonb) as position
    from input_item
    group by username, code, updated_at
)
, input_lag as (
    select username, code, updated_at, position,
        lag(position) over (partition by username, code order by updated_at) as prev_position
    from input_repack
)
, prev_pos_unpack as (
    select username, code, updated_at, position, prev_item
    from input_lag
    left join lateral (
        select jsonb_array_elements(coalesce(prev_position, '[]'::jsonb)) as prev_item
    ) as lp on true
)
, prev_pos_zeroed as (
    select username, code, updated_at, position,
    jsonb_set(prev_item, '{amount}', '0'::jsonb) as prev_item_zeroed
    from prev_pos_unpack
)
, zeroed_pack as (
    select username, code, updated_at, position,
    coalesce(
        jsonb_agg(prev_item_zeroed) filter (where prev_item_zeroed is not null),
        '[]'::jsonb) as prev_position_zeroed
    from prev_pos_zeroed
    group by username, code, updated_at, position
)
, spirit_position as (
    select username, code, updated_at,
    case when position is null or jsonb_array_length(position) = 0 then prev_position_zeroed
    else position end as position_filled
    from zeroed_pack
)
, position_unpack as (
    select username, code, updated_at, item
    from spirit_position
    left join lateral (
        select jsonb_array_elements(position_filled) as item
    ) as lp on true
)
, position_final as (
    select username, code, updated_at,
    item->>'contract' as contract,
    (item->'amount')::int as amount
    from position_unpack
)
, position_aggregated as (
    select updated_at as time,
    username || '@' || coalesce(contract, '<empty>') as tag,
    coalesce(sum(amount) filter (where amount is not null), 0) as amount
    from position_final
    group by updated_at, username, contract
    order by updated_at asc
)
select * from position_aggregated
order by time;
