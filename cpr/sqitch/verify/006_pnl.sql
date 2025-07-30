-- Verify cpr:006_pnl on pg

BEGIN;

-- XXX Add verifications here.
-- Verify that the cpr.clip_trade_profit table exists.
select 1 from cpr.clip_trade_profit limit 1;

-- 这里我们需要插入一些测试数据来验证函数的正确性。
-- 包括 dataset, clip_trade_args, clip_trade_backtest 的数据。

ROLLBACK;
