-- Revert cpr:014_hb_record from pg

BEGIN;

-- XXX Add DDLs here.

drop table if exists hb.trade_record cascade;
drop table if exists hb.position_record cascade;
drop table if exists hb.account_record cascade;

COMMIT;
