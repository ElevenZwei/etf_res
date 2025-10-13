-- Title: cpr_signal

-- $roll_export_id_list defined as:
select id from cpr.roll_export
where roll_args_id = $roll_args_id and top = $top
and dt_to >= to_timestamp($__from / 1000) and dt_from <= to_timestamp($__to / 1000)

-- Panel: cpr signal 
with s1 as (
  select dt, $__timeGroup(dt, $__interval) as time, roll_export_id, position
  from cpr.roll_export_run
  where roll_export_id in ($roll_export_id_list)
  and $__timeFilter(dt)
)
select distinct on(time, tag)
time, 'with reid ' || roll_export_id as tag, position
from s1 order by time, tag, dt asc;

-- Panel: cpr signal latest
with reid as ($roll_export_id_list order by dt_from desc limit 1)
select position 
from cpr.roll_export_run rer join reid on rer.roll_export_id = reid.id
and $__timeFilter(dt)
order by dt desc
limit 1;

-- Panel: cpr signal execution history from spirit persistence, first value
with input as (
  select updated_at as time,
    username, key, value
  from hb.spirit_persistence_history
  where $__timeFilter(updated_at)
    and spirit = 'CprFetch'
)
select
  time, username || '@' || key as tag, value::float8 as position
from input
order by time asc, username asc, key asc;

