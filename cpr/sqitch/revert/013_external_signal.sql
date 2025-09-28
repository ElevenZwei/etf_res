-- Revert cpr:013_external_signal from pg

BEGIN;

-- XXX Add DDLs here.
drop trigger if exists set_modified_time on cpr.stock_signal;
drop function if exists cpr.update_column_modified_time();
drop trigger if exists upsert_stock_signal on cpr.stock_signal;
drop function if exists cpr.upsert_stock_signal();
drop index if exists idx_stock_signal_product;
drop table if exists cpr.stock_signal cascade;

COMMIT;
