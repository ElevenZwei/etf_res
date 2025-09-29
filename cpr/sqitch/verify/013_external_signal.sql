-- Verify cpr:013_external_signal on pg

BEGIN;

-- XXX Add verifications here.
do $$
declare
    row_id integer;
    sig_time timestamptz;
    modi_time timestamptz;
begin
    insert into cpr.stock_signal (product, acname, ps)
        values ('test', 'test_st', 0.1)
        returning id into row_id;
    perform assert(count(*) = 1, 'Expected one row')
        from cpr.stock_signal
        where product = 'test' and acname = 'test_st'
            and ps = 0.1 and if_final = 1;
    select insert_time, modified_time into sig_time, modi_time
        from cpr.stock_signal
        where id = row_id;
    insert into cpr.stock_signal (product, acname, insert_time, ps)
        values ('test', 'test_st', sig_time, 0.2);
    perform assert(ps = 0.2, 'Expected new position value')
        from cpr.stock_signal
        where product = 'test' and acname = 'test_st'
            and insert_time = sig_time and if_final = 1;
    -- we cannot test modified_time, because now() will be kept same in one statement.
end; $$;


ROLLBACK;
