-- Verify cpr:002_cpr on pg

BEGIN;

-- XXX Add verifications here.
select 1 from cpr.cpr limit 1;

insert into cpr.dataset (spotcode, expiry_priority, strike)
values ('TESTCODE', 1, 100.00);

select assert(count(*) = 1, 'Expected one row in cpr.dataset after insert')
from cpr.dataset
where spotcode = 'TESTCODE' and expiry_priority = 1 and strike = 100.00;

do $$
declare
    ds integer;
    diff float8;
begin
    select id into ds from cpr.dataset
        where spotcode = 'TESTCODE' and expiry_priority = 1 and strike = 100.00;
    -- raise notice 'dataset_id: %', ds;
    insert into cpr.cpr (dt, "dataset_id", call, put) values
        ('2023-10-01 10:00:00+08', ds, 10, 5),
        ('2023-10-01 11:00:00+08', ds, 5, 10),
        ('2023-10-02 10:00:00+08', ds, 10, 10),
        ('2023-10-02 11:00:00+08', ds, 5, 10);
    perform assert(count(*) = 2, 'Expected two rows in cpr.cpr after insert')
        from cpr.cpr
        where dt::date = '2023-10-01' and "dataset_id" = ds;

    perform cpr.update_daily('2023-10-01', '2023-10-02', ds, notice => false);
    select ratio_diff into diff from cpr.daily
        where dt = '2023-10-01 11:00:00+08' and "dataset_id" = ds;
    perform assert(diff = -2.0/3.0, 'Expected ratio_diff to be -2/3 for 2023-10-01 11:00:00+08');
    select ratio_diff into diff from cpr.daily
        where dt = '2023-10-02 11:00:00+08' and "dataset_id" = ds;
    perform assert(diff = -1.0/3.0, 'Expected ratio_diff to be -1/3 for 2023-10-02 11:00:00+08');
end $$;

ROLLBACK;
