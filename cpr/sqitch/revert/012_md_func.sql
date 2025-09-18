-- Revert cpr:012_md_func from pg

BEGIN;

-- XXX Add DDLs here.

drop function if exists md.get_contract_of_date(text, date);
drop function if exists md.list_contracts_of_date(date);
drop function if exists md.upsert_contract_info(
    text, text, text, text, integer, text, float8, integer, date);
drop function if exists md.update_contract_price_daily(
    text, timestamptz, float8, bigint, integer);
drop function if exists md.update_contract_price_minute(
    text, timestamptz, float8, bigint, integer);
drop function if exists md.update_contract_price(
    text, timestamptz, float8, bigint, integer);


COMMIT;
