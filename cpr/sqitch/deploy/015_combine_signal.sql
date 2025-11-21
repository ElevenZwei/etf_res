-- Deploy cpr:015_combine_signal to pg

BEGIN;

-- XXX Add DDLs here.

-- 组合信号的方案信息表格
create table if not exists cpr.combine_signal_scheme (
    id serial primary key,
    -- 方案名称
    scheme_name text not null unique,
    -- 方案描述
    description text not null,
    -- 方案代码
    code text,
    -- 创建时间
    created_at timestamptz not null default now(),
    -- 最后修改时间
    updated_at timestamptz not null default now()
);

create or replace function cpr.update_combine_signal_scheme_modified_time()
returns trigger language plpgsql as $$
begin
    if row(new.*) is distinct from row(old.*) then
        new.updated_at = now();
        return new;
    end if;
    -- return old 会取消写入过程并取消任何后续事件触发。
    return old;
end; $$;

create trigger set_combine_signal_scheme_updated_time
    before update on cpr.combine_signal_scheme
    for each row execute function cpr.update_combine_signal_scheme_modified_time();

create or replace function cpr.get_or_create_combine_signal_scheme(
    scheme_name_arg text, description_arg text, code_arg text)
    returns integer language plpgsql as $$
declare
    scheme_id integer;
begin
    select id into scheme_id from cpr.combine_signal_scheme
        where scheme_name = scheme_name_arg;
    if scheme_id is not null then
        update cpr.combine_signal_scheme
            set description = description_arg,
                code = code_arg
            where id = scheme_id;
        return scheme_id;
    end if;
    -- insert new record
    -- let the caller retry if conflict happens
    insert into cpr.combine_signal_scheme (scheme_name, description, code)
        values (scheme_name_arg, description_arg, code_arg)
        returning id into scheme_id;
    return scheme_id;
end; $$;


-- 组合信号数据表格
create table if not exists cpr.combine_signal (
    id serial primary key,
    scheme_id integer not null references cpr.combine_signal_scheme(id) on delete cascade,
    dt timestamptz not null,
    product text not null,
    position float8 not null default 0,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check(position >= -1 and position <= 1)
);
create unique index if not exists idx_combine_signal_scheme_dt_product
    on cpr.combine_signal(scheme_id, dt, product);

create or replace function cpr.update_combine_signal_modified_time()
returns trigger language plpgsql as $$
begin
    if row(new.*) is distinct from row(old.*) then
        new.updated_at = now();
        return new;
    end if;
    -- return old 会取消写入过程并取消任何后续事件触发。
    return old;
end; $$;

create trigger set_combine_signal_updated_time
    before update on cpr.combine_signal
    for each row execute function cpr.update_combine_signal_modified_time();

COMMIT;
