-- Deploy cpr:005_market to pg

BEGIN;

-- XXX Add DDLs here.
create table cpr.market_minute(
    dt timestamptz not null,
    code text not null,
    openp numeric(12, 4) not null,
    closep numeric(12, 4) not null,
    highp numeric(12, 4) not null,
    lowp numeric(12, 4) not null
);
create unique index on cpr.market_minute (dt, code);

-- 上面这个表格的数据来源可以是 Wind 或者从另一个数据库的 market_data_tick tick -> bar 合成。
-- 因为 Call Put Ratio 数据基本上都是那一分钟的 open snapshot ，所以交易价格可以使用同一分钟的 closep 。


COMMIT;
