-- Deploy cpr:016_external_signal_archive to pg

BEGIN;

-- XXX Add DDLs here.
create table if not exists cpr.stock_signal_import (
    dt timestamptz not null,
    name text not null,  -- strategy name
    code text not null,  -- stock code
    position float8 not null,  -- position [-1, 1]
    check (position >= -1 and position <= 1)
);

create index if not exists idx_stock_signal_import_dt_name_code
    on cpr.stock_signal_import (dt, name, code);
create index if not exists idx_stock_signal_import_name_code_dt
    on cpr.stock_signal_import (name, code, dt);


COMMIT;
