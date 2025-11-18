-- Deploy cpr:013_external_signal to pg

BEGIN;

-- XXX Add DDLs here.

-- 股票组写入信号数据的表格，数据列名称按他们现有的风格。
-- Stock Signal in realtime
create table if not exists cpr.stock_signal (
    -- 股票代码，例如 399006
    id serial primary key,
    -- 产品名称
    product text not null,
    name text,
    -- 策略名称
    acname text not null,
    -- 信号触发计算的时间点
    insert_time timestamptz not null default now(),
    -- 信号真实写入的时间点，通常比 insert_time later 30 seconds.
    modified_time timestamptz not null default now(),
    -- position, range [-1, 1]
    ps float8 not null default 0,
    -- 是否是最新一个信号
    if_final int2 not null default 1,
    check(ps >= -1 and ps <= 1)
);

create unique index if not exists idx_stock_signal_product
    on cpr.stock_signal(product, acname, insert_time);

-- update trigger
create or replace function cpr.update_column_modified_time()
returns trigger language plpgsql as $$
begin
    if row(new.*) is distinct from row(old.*) then
        new.modified_time = now();
        return new;
    end if;
    -- return old 会取消写入过程并取消任何后续事件触发。
    return old;
end;
$$;

create trigger set_modified_time
    before update on cpr.stock_signal
    for each row execute function cpr.update_column_modified_time();

-- insert update-columns trigger
create or replace function cpr.upsert_stock_signal()
returns trigger language plpgsql as $$
declare
    old_id integer;
begin
    select id into old_id from cpr.stock_signal
        where product = new.product and acname = new.acname and insert_time = new.insert_time;
    if found then
        update cpr.stock_signal set
            ps = new.ps,
            if_final = new.if_final,
            modified_time = now()
        where id = old_id;
        return null;
    end if;
    return new;
end;
$$;

create trigger upsert_stock_signal
before insert on cpr.stock_signal
for each row execute function cpr.upsert_stock_signal();

COMMIT;
