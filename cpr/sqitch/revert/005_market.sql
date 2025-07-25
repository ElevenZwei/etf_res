-- Revert cpr:005_market from pg

BEGIN;

-- XXX Add DDLs here.

drop table if exists cpr.market_minute cascade;

COMMIT;
