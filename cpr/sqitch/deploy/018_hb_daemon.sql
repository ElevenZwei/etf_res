-- Deploy cpr:018_hb_daemon to pg

BEGIN;

-- XXX Add DDLs here.

-- 记录守护进程实例的信息，每个实例对应一个唯一的配置文件和用户名组合。
-- 在每次守护进程启动时插入一条记录。
create table if not exists hb.daemon_instance (
    id serial primary key,
    dt timestamptz not null default now(),
    daemon_pid int not null,
    script_filename text,
    exe_filename text not null,
    exe_fallback_filename text,
    config_filename text not null,
    config_name text not null,
    username text not null
);

create index if not exists idx_daemon_instance_username_config
    on hb.daemon_instance (username, config_name);

-- 记录守护进程的心跳信息，每次守护进程发送心跳时插入一条记录。
create table if not exists hb.daemon_heartbeat (
    id serial primary key,
    daemon_instance_id int not null references hb.daemon_instance(id) on delete cascade,
    dt timestamptz not null default now(),
    counter int not null
);

-- 这是一个日志类型的表格，用于记录守护进程的心跳信息。
-- 不要在这个表格上定义唯一约束，这里接受并发写入，允许覆盖更新。
create index if not exists idx_daemon_heartbeat_dt_daemon_args
    on hb.daemon_heartbeat (dt, daemon_instance_id);

-- 记录守护进程执行的操作，每次守护进程执行某个操作时插入一条记录。
create table if not exists hb.daemon_action (
    id serial primary key,
    dt timestamptz not null default now(),
    daemon_instance_id int not null references hb.daemon_instance(id) on delete cascade,
    exe_fallback boolean not null default false,
    exe_pid int not null,
    action text not null
);

create index if not exists idx_daemon_action_dt_daemon_args
    on hb.daemon_action (dt, daemon_instance_id);

COMMIT;
