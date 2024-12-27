-- 这个是从 MySQL Nifty 数据库里面读取需要的数据的 SQL 代码。
-- spot csv: dt, code, price, oicall, oiput, oicp
-- greeks csv: dt, code, tradecode, closep, openinterest, rootcode, strike, expirydate, impv, delta

-- 这里的 Price 指 open 还是 close 不知道，也不重要，因为只要两个文件的时间是同步的。

-- Nifty Spot data
-- 因为老版本 MySQL 的优化是非常过激地烂，所以只能用 subquery 来解决。
set @bgdt = '2024-09-01';
set @eddt = '2024-12-31';
select
    dt, 'NIFTY' as code, spot as price,
    oicall, oiput,
    oicall - oiput as oicp
from (
	select dt, spot, oicall, oiput
	from (
		select `time` as dt, spot_price as spot,
			sum(oi_calls) / 2 as oicall,
			sum(oi_puts) / 2 as oiput
		from nifty join nifty_spot_price nsp using(`time`)
		where `time` between @bgdt and @eddt
		and strike is null
		group by `time`, spot_price
		order by `time` asc
	) as t1
) as t2
order by dt asc;

-- Nifty Greeks data

select dt,
    concat('NIFTY-', IF(cp = 1, 'C', 'P'), '-', DATE_FORMAT(expirydate, '%Y%m%d'), '-', strike) as code,
    closep, openinterest,
    'NIFTY' as rootcode, strike, expirydate, impv, delta
from ((select
        `time` as dt,
        ltp_calls as closep,
        oi_calls as openinterest,
        strike,
        1 as cp,
        STR_TO_DATE(nsp.expiry_date, '%d %b, %Y') as expirydate,
        iv_calls as impv,
        delta_calls as delta
        from nifty join nifty_spot_price nsp using (`time`)
        where `time` between @bgdt and @eddt
        and delta_calls is not null
        and iv_calls > 0)
    union
    (select
        `time` as dt,
        ltp_puts as price,
        oi_puts as openinterest,
        strike,
        -1 as cp,
        STR_TO_DATE(nsp.expiry_date, '%d %b, %Y') as expirydate,
        iv_puts as impv,
        delta_puts as delta
        from nifty join nifty_spot_price nsp using (`time`)
        where `time` between @bgdt and @eddt
        and delta_puts is not null
        and iv_puts > 0)
) t1;

