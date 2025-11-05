-- Queries for monitoring accounts.

-- Description: Query to monitor accumulated trade amounts for a specific user and contract within a date range.
select
dt, username, contract_name, amount, direction,
sum(amount * direction) over (partition by username, contract_name order by dt) as amount_accumulated
from "hb"."trade_record"
where dt between '2025-11-03' and '2025-11-04'
and username = '505000011915';

-- Grafana Variation
with input as (
    select dt, username, contract_name, amount, direction,
    sum(amount * direction) over (partition by username, contract_name order by dt) as amount_accumulated
    from "hb"."trade_record"
    where $__timeFilter(dt)
    and username = '505000011915'
)
select
dt as time, username || '@' || contract_name as tag,
amount_accumulated as accu
from input
order by dt asc, username asc, contract_name asc;




