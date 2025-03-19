-- ddl part

create table trade_strategy(
    st_name text primary key,
    code text,
    st_desc text
);

create table trade_strategy_args(
    arg_desc text not null,
    arg_spot text not null,
    arg jsonb,
    st_name text references trade_strategy(st_name) on update cascade on delete cascade,
    create_dt timestamptz not null default now(),
    primary key(arg_desc, arg_spot)
);

create table trade_signal(
    arg_desc text not null,
    arg_spot text not null,
    dt timestamptz not null,
    act int not null,
    primary key(arg_desc, dt),
    foreign key(arg_desc, arg_spot) references trade_strategy_args(arg_desc, arg_spot)
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

