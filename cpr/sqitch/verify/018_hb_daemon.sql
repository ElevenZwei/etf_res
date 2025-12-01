-- Verify cpr:018_hb_daemon on pg

BEGIN;

-- XXX Add verifications here.

do $$
declare
    inst_id int;
begin
    insert into hb.daemon_instance (
        daemon_pid,
        script_filename,
        exe_filename,
        exe_fallback_filename,
        config_filename,
        config_name,
        username
    ) values (
        12345,
        'test_script.sh',
        'test_exe',
        'test_exe_fallback',
        'test_config.conf',
        'test_config',
        'test_user'
    ) returning id into inst_id;
    
    
end;
$$;


ROLLBACK;
