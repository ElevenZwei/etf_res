-- Title: stock_signal

-- Panel: stock signal elements.
-- #1
with input as (
  select $__timeGroup(insert_time, $__interval) as time,
  acname, ps
  from cpr.stock_signal
  where product = '399006' and $__timeFilter(insert_time)
)
select time,
avg(ps) filter (where acname = 'pyelf_CybWoOacVswRmRR_sif_1_1') as "CybWoOacVswRmRR_sif_1_1",
avg(ps) filter (where acname = 'pyelf_CybWoOacVswRm_sif_1_1') as "CybWoOacVswRm_sif_1_1",
avg(ps) filter (where acname = 'pyelf_CybWoOacVswRR_sif_1_1') as "CybWoOacVswRR_sif_1_1",
avg(ps) filter (where acname = 'pyelf_CybWoOacVsw_sif_1_1') as "CybWoOacVsw_sif_1_1"
from input
group by time
order by time;

-- #2 format as "timeseries", first value of interval
with input as (
  select modified_time, $__timeGroup(modified_time, $__interval) as time,
  acname, ps
  from cpr.stock_signal
  where product = '399006' and $__timeFilter(insert_time)
)
select distinct on (time, acname)
time, replace(acname, 'pyelf_', '') as tag, ps as pos
from input
order by time asc, acname asc, modified_time asc


-- Panel: stock signal average, average value of interval.
with input as (
  select $__timeGroup(insert_time, $__interval) as time,
  acname, ps
  from cpr.stock_signal
  where product = '399006' and $__timeFilter(insert_time)
)
select time, avg(ps) as position
from input
group by time

-- Panel: latest stock signal
select avg(ps) as value
from cpr.stock_signal
where product = '399006' and $__timeFilter(insert_time)
group by insert_time
order by insert_time desc
limit 1;

-- Panel: stock signal execution history from spirit persistence.
with input as (
  select updated_at as time,
    username, key, value
  from hb.spirit_persistence_history
  where $__timeFilter(updated_at)
    and spirit = 'StockFetch'
)
select
  time, username || '@' || key as tag, value::float8
from input
order by time asc, username asc, key asc;
