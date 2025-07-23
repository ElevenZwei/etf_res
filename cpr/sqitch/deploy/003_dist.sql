-- Deploy cpr:003_dist to pg

BEGIN;

-- XXX Add DDLs here.
create table cpr.dist (
    id serial primary key,
    dataset_id integer not null references cpr.dataset(id) on delete cascade,
    dt timestamptz not null,
    ti time not null,
    call integer not null,
    put integer not null,
    ratio float8 not null
);

COMMIT;
