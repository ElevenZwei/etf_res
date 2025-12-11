-- Revert cpr:010_persistence from pg

BEGIN;

-- XXX Add DDLs here.
drop table if exists hb.spirit_persistence cascade;
drop table if exists hb.spirit_persistence_history cascade;
drop table if exists hb.spirit_position cascade;
drop table if exists hb.spirit_position_history cascade;

COMMIT;
