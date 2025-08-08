-- Deploy cpr:008_roll to pg

BEGIN;

-- XXX Add DDLs here.

-- Roll Method 需要包含它筛选以及评估 trade args 的方案和参数。
-- Roll Method 的执行者需要读取 cpr.clip_trade_args 表的数据，先执行一轮筛选，
-- 然后再读取 cpr.clip_trade_profit 表的数据，计算出每个 args 的表现状况。
-- 根据 args 的表现状况，剔除不合理的表现，最后得出一个排序好的 args 列表。
create table if not exists cpr.roll_method (
    id serial primary key,
    name text not null,
    variation text not null,
    is_static boolean not null,
    args jsonb,
    description text,
    created_at timestamptz not null default now()
);
create unique index if not exists
roll_method_main_idx
on cpr.roll_method (name, variation);

-- Roll Args 是运行 Roll Method 需要的补充参数。
-- Roll Args 包含了这一轮运行筛选的 trade args 的范围，以及需要从中选取的 args 数量。
-- 这一步运用了 trade args id 只增加不减少的特性，保证新增的 trade args 不会对之前的结果产生影响。
create table if not exists cpr.roll_args (
    id serial primary key,
    dataset_id integer not null references cpr.dataset(id) on delete cascade,
    roll_method_id integer not null references cpr.roll_method(id) on delete cascade,
    trade_args_from_id integer not null,
    trade_args_to_id integer not null,
    pick_count integer not null,
    created_at timestamptz not null default now()
);
create unique index if not exists
roll_args_main_idx
on cpr.roll_args (
    dataset_id, roll_method_id,
    trade_args_from_id, trade_args_to_id,
    pick_count);

-- Roll Rank 是 Roll Method 执行的结果，包含一个策略的 predict rank 和 real rank。
create table if not exists cpr.roll_rank (
    roll_args_id integer not null references cpr.roll_args(id) on delete cascade,
    trade_args_id integer not null references cpr.clip_trade_args(id) on delete cascade,
    train_dt_from timestamptz not null,
    train_dt_to timestamptz not null,
    validate_dt_from timestamptz not null,
    validate_dt_to timestamptz not null,

    predict_rank integer not null,  -- Roll Method 根据 train 对 validate 预测排名，from 1
    predict_weight integer not null,  -- Roll Method 对 validate 预测权重
    predict_score float8 not null,  -- Roll Method 对 validate 预测分数

    -- validate 数据的实际排名，或者说 Roll Method 看到了这些数据之后会给出的排名，from 1
    real_rank integer not null,
    -- validate 数据的实际权重，或者说 Roll Method 看到了这些数据之后会给出的权重
    real_weight integer not null,
    -- validate 数据的实际分数，或者说 Roll Method 看到了这些数据之后会给出的分数
    real_score float8 not null,

    created_at timestamptz not null default now()
);
-- Roll Rank 的主键是 roll_args_id, trade_args_id, train_dt_from 。
-- 其中 train_dt_from 是 Roll Method 执行时的 train 数据的起始时间。
-- 这里的限制严格一些。
-- 如果要对同一段 train 做出不同长度的 validate 预测，
-- 或者对同一个 train 起点使用不同的 train length，
-- 那么需要新建多个不同的 roll_args_id。
create unique index if not exists
roll_rank_main_idx
on cpr.roll_rank (
    roll_args_id, trade_args_id, train_dt_from);

-- Roll Result 是 Roll Method 从 train data 运行出 predict rank 应用到验证数据上的结果。
create table if not exists cpr.roll_result (
    roll_args_id integer not null references cpr.roll_args(id) on delete cascade,
    trade_args_id integer not null references cpr.clip_trade_args(id) on delete cascade,
    dt_from timestamptz not null,
    dt_to timestamptz not null,
    predict_rank integer not null,  -- Roll Method 对同一段 dt 中不同 trade args 里的排名，from 1
    predict_weight integer not null,
    created_at timestamptz not null default now()
);
create unique index if not exists
roll_result_main_idx
on cpr.roll_result (
    roll_args_id, trade_args_id, dt_from);
create unique index if not exists
roll_result_rank_idx
on cpr.roll_result (
    roll_args_id, dt_from, predict_rank);

-- Roll Merged 是 Roll Result 的合并结果。
create table if not exists cpr.roll_merged (
    roll_args_id integer not null references cpr.roll_args(id) on delete cascade,
    top integer not null,  -- 选择 roll_result.predict_rank <= top 的 trade args
    dt timestamptz not null,
    position float8 not null
);
create unique index if not exists
roll_merged_main_idx
on cpr.roll_merged (roll_args_id, top, dt);

create or replace function cpr.get_or_create_roll_method (
    name_arg text, variation_arg text,
    is_static_arg boolean, args_arg jsonb, description_arg text)
returns integer language plpgsql as $$
declare
    method_id integer;
begin
    select id into method_id from cpr.roll_method
        where name = name_arg and variation = variation_arg;
    if method_id is not null then
        update cpr.roll_method
            set is_static = is_static_arg,
            args = args_arg,
            description = description_arg
            where id = method_id;
            return method_id;
    end if;
    insert into cpr.roll_method (name, variation, is_static, args, description)
        values (name_arg, variation_arg, is_static_arg, args_arg, description_arg)
        on conflict (name, variation) do update
        set is_static = is_static_arg,
        args = excluded.args,
        description = excluded.description
        returning id into method_id;
    if method_id is null then
        select id into method_id from cpr.roll_method
            where name = name_arg and variation = variation_arg;
    end if;
    return method_id;
end; $$;

create or replace function cpr.get_or_create_roll_args (
    dataset_id_arg integer, roll_method_id_arg integer,
    trade_args_from_id_arg integer, trade_args_to_id_arg integer,
    pick_count_arg integer)
returns integer language plpgsql as $$
declare
    args_id integer;
begin
    select id into args_id from cpr.roll_args
        where dataset_id = dataset_id_arg
        and roll_method_id = roll_method_id_arg
        and trade_args_from_id = trade_args_from_id_arg
        and trade_args_to_id = trade_args_to_id_arg
        and pick_count = pick_count_arg;
    if args_id is not null then
        return args_id;
    end if;
    insert into cpr.roll_args (dataset_id, roll_method_id,
        trade_args_from_id, trade_args_to_id, pick_count)
        values (dataset_id_arg, roll_method_id_arg,
            trade_args_from_id_arg, trade_args_to_id_arg, pick_count_arg)
        on conflict do nothing
        returning id into args_id;
    if args_id is null then
        select id into args_id from cpr.roll_args
            where dataset_id = dataset_id_arg
            and roll_method_id = roll_method_id_arg
            and trade_args_from_id = trade_args_from_id_arg
            and trade_args_to_id = trade_args_to_id_arg
            and pick_count = pick_count_arg;
        end if;
    return args_id;
end; $$;


COMMIT;
