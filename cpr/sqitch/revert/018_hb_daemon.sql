-- Revert cpr:018_hb_daemon from pg

BEGIN;

-- XXX Add DDLs here.
drop table if exists hb.daemon_action cascade;
drop table if exists hb.daemon_heartbeat cascade;
drop table if exists hb.daemon_instance cascade;

COMMIT;
