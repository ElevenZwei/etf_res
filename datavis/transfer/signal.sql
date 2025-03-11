-- ddl part

create table trade_strategy(
    name text primary key,
    code text,
    description text
);

create table trade_strategy_args(
    arg_id int generated always as identity primary key,
    name text references trade_strategy(name) on update cascade on delete cascade,
    args jsonb
);

create table trade_signal(
    arg_id int references trade_strategy_args(arg_id) on update cascade on delete cascade,
    dt timestamptz not null,
    act int not null,
    constraint pk_trade_signal primary key(arg_id, dt)
);

-- oi 中间分析用表
create table oi_analysis(
    dt timestamptz not null,
    spot text,
    ts int4,
    sigma float4,
    val int4,
    constraint pk_oi_analysis primary key(dt, spot, ts, sigma)
);

select create_hypertable('oi_analysis', by_range('dt'));

