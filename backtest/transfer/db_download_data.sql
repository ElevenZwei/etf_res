-- 这个脚本用来处理新版的 Nautilus 需要的回测数据。

-- 我用一个脚本抽取这段时间的最近到期 options 和 etf 数据
-- 注意 contract_info 里面现在有两个格式的数据，
-- 一类是 159915.SZ 这样有后缀的，这个是以前累积以及从 Wind 导入的，
-- 一类是 159915 没有后缀的，这个是录制机器录入的数据。

@set bgdt='2024-01-01'
@set eddt='2024-11-01'
@set rootcode='159915.SZ'
--explain analyze
with dt_contract as (
	select dt, rootcode, code, tradecode, strike, expirydate
	from soon_expire se
	join contract_info ci
	on se.rootcode = ci.spotcode and se.edt = ci.expirydate
	where se.dt >= $bgdt and se.dt <= $eddt
	and rootcode = $rootcode
)
select md.dt, md.code, tradecode, closep, openinterest, rootcode, strike, expirydate from market_data md
join dt_contract on md.dt::date = dt_contract.dt and md.code = dt_contract.code
where md.dt >= $bgdt and md.dt <= $eddt;

select expirydate, count(*) as cnt
from contract_info ci 
group by expirydate;

-- 这个代码是用来更正本来错误设置在 2024-03-08 上面的许多合约。
update contract_info
set expirydate = T3.last_day
from (
	select (first_day + interval '1 day' * ((3 - EXTRACT(DOW FROM first_day) + 7) % 7) + interval '21 days')::date as last_day, *
	from (
		select make_date(yr, mn, 1) as first_day, *
		from (
			select code,
			substring(tradecode, 8, 2)::int + 2000 as yr,
			substring(tradecode, 10, 2)::int as mn
			from contract_info ci 
			where expirydate = '2024-03-08'
		) T
	) T2
) T3
where contract_info.code = T3.code;

-- 这个是计算第四个星期三的日期的示例代码，这种 with 写法因为可以尊重从上往下的流水线，所以比嵌套 from 更加清晰。
WITH first_day AS (
    -- 获取指定年份和月份的第一天
    SELECT DATE '2024-11-01' AS first_day
), fourth_wednesday AS (
    -- 计算第四个星期三的日期
    SELECT first_day + interval '1 day' * ((3 - EXTRACT(DOW FROM first_day) + 7) % 7) + interval '21 days' AS fourth_wednesday_date
    FROM first_day
)
SELECT fourth_wednesday_date
FROM fourth_wednesday;


