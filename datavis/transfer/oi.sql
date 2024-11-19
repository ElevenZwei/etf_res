-- 这个是查询日内期权的 OI 变化，然后我可以使用 Plotly 进行三维绘图
-- 这个是 oi 数据的关键来源
-- 关键是一定要在 session 里面把 nestloop 关掉逼迫它使用 parallel bitmap scan 查询大表
-- 这可以带来数十倍的速度提升
set enable_nestloop = off;
@set bgdt='2024-10-18T09:30:00'
@set eddt='2024-10-18T15:00:00'
-- explain 
with OI as (
select *, oi - oi_open as oi_diff from (
	select
		dt, spotcode, expirydate, callput, strike, tradecode,
		open_interest as oi,
		first_value(open_interest) over (partition by code order by dt) as oi_open
	from market_data_tick mdt join contract_info ci using(code)
	where dt >= $bgdt and dt <= $eddt
	and spotcode = '159915' and expirydate = '2024-10-23') as T
)
select od.*, mdt.last_price as spot_price from (
	select
		oi1.dt, spotcode, expirydate, strike,
		oi1.tradecode as oi1c, oi2.tradecode as oi2c,
		oi1.oi_diff as oi_diff_c, oi2.oi_diff as oi_diff_p
	from OI oi1 join OI oi2 using (dt, spotcode, expirydate, strike)
	where oi1.callput = 1 and oi2.callput = -1) as od
join market_data_tick mdt using (dt)
where mdt.code = od.spotcode
and dt >= $bgdt and dt <= $eddt
order by dt asc;

-- 下面这个写法没有变量代入剪枝更慢一些
with OI as (
select *, oi - oi_open as oi_diff from (
select dt, spotcode, expirydate, callput, strike, tradecode,
open_interest as oi,
first_value(open_interest) over (partition by code order by dt) as oi_open
from market_data_tick mdt join contract_info ci using(code)
where dt >= '2024-10-21T09:30:00' and dt <= '2024-10-21T15:00:00'
and spotcode = '159915' and expirydate = '2024-10-23') as T)
select od.*, mdt.last_price as spot_price from (
	select oi1.dt, spotcode, expirydate, strike,
	oi1.tradecode as oi1c, oi2.tradecode as oi2c,
	oi1.oi_diff as oi_diff_c, oi2.oi_diff as oi_diff_p
	from OI oi1 join OI oi2 using (dt, spotcode, expirydate, strike)
	where oi1.callput = 1 and oi2.callput = -1) as od
join market_data_tick mdt using (dt)
where mdt.code = od.spotcode
order by dt asc;
