-- Revert cpr:015_combine_signal from pg

BEGIN;

-- XXX Add DDLs here.
drop function if exists cpr.get_or_create_combine_signal_scheme(text, text, text);
drop trigger if exists set_combine_signal_scheme_updated_time on cpr.combine_signal_scheme;
drop function if exists cpr.update_combine_signal_scheme_modified_time();
drop table if exists cpr.combine_signal_scheme cascade;

drop trigger if exists set_modified_time on cpr.stock_signal;
drop function if exists cpr.update_column_modified_time();
drop table if exists cpr.combine_signal cascade;

COMMIT;
