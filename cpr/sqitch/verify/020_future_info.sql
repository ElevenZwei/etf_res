-- Verify cpr:020_future_info on pg

BEGIN;

-- XXX Add verifications here.
do $$
begin
    insert into md.future_contract(tradecode, commodity)
        values ('TEST_COMM604', 'TEST_COMM');
    perform assert(md.get_future_commodity('TEST_COMM604') = 'TEST_COMM',
        'md.get_future_commodity should return correct commodity');
    insert into md.future_main(tradecode, commodity, valid_from)
        values
        ('TEST_COMM603', 'TEST_COMM', '2026-01-01'),
        ('TEST_COMM604', 'TEST_COMM', '2026-03-01');
    perform assert(md.get_future_main('TEST_COMM', '2026-02-01') = 'TEST_COMM603',
        'md.get_future_main should return correct main contract for 2026-02-01');
    perform assert(md.get_future_main('TEST_COMM', '2026-03-01') = 'TEST_COMM604',
        'md.get_future_main should return correct main contract for 2026-03-01');
end; $$;

ROLLBACK;
