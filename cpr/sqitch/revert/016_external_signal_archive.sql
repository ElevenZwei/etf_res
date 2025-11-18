-- Revert cpr:016_external_signal_archive from pg

BEGIN;

-- XXX Add DDLs here.
drop table if exists cpr.stock_signal_import cascade;

COMMIT;
