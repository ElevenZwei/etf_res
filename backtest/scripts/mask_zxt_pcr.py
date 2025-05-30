"""
这个脚本的作用是：
对 ZXT 提供的 PCR 信号，做一个 PC DIFF 固定阈值的遮罩裁剪，
把新的信号储存起来。
"""

import pandas as pd
from backtest.config import DATA_DIR

BEGIN_DATE = '2023-01-01'
END_DATE = '2024-12-31'

# 这个数据是 ZXT 提供的 PCR 信号
df_zxt_pcr = pd.read_parquet(f'{DATA_DIR}/zxt/pc_final_pos.parquet', engine='pyarrow')
df_zxt_pcr = df_zxt_pcr.rename_axis('dt')
df_zxt_pcr.index = df_zxt_pcr.index.tz_localize('Asia/Shanghai')
df_zxt_pcr = df_zxt_pcr[df_zxt_pcr.index >= BEGIN_DATE]
df_zxt_pcr = df_zxt_pcr.rename(columns={'159915': 'pcr'})
df_zxt_pcr['spot'] = '159915'

df_zxt_stock = pd.read_parquet(f'{DATA_DIR}/zxt/ofsignal_pos.parquet', engine='pyarrow')
df_zxt_stock = df_zxt_stock.rename_axis('dt')
df_zxt_stock.index = df_zxt_stock.index.tz_localize('Asia/Shanghai')
df_zxt_stock = df_zxt_stock[df_zxt_stock.index >= BEGIN_DATE]
df_zxt_stock.to_csv(f'{DATA_DIR}/input/zxt_stock.csv', index=True)
df_zxt_stock['stock'] = df_zxt_stock.mean(axis=1)
# df_zxt_stock['stock'] = df_zxt_stock['stock'] * 2 - 1  # convert [0, 1] to [-1, 1]
df_zxt_stock['spot'] = '159915'
df_zxt_stock = df_zxt_stock[['spot', 'stock']]

# 这里还要再加一步，从 PCR 到 PCR POSITION，
# 例如 PCR > 0.8 的时候开仓，在 PCR < 0.2 的时候平仓
class PCRPosition:
    def __init__(self, open_th: float, close_th: float):
        self.open_th = open_th
        self.close_th = close_th
        self.pos = 0

    def __call__(self, pcr: float) -> int:
        if self.pos == 0:
            if pcr > self.open_th:
                self.pos = 1
            if pcr < -self.open_th:
                self.pos = -1
        if self.pos == 1 and pcr < self.close_th:
            self.pos = 0
        if self.pos == -1 and pcr > -self.close_th:
            self.pos = 0
        return self.pos

def calc_position(df: pd.DataFrame, open_th: float, close_th: float, column: str) -> pd.Series:
    helper = PCRPosition(open_th, close_th)
    df[column + '_position'] = df[column].apply(helper)
    return df

def calc_position_changes(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    column = prefix + '_position'
    df[column + '_prev'] = df[column].shift(1)
    df[column + '_prev'] = df[column + '_prev'].fillna(0)
    changes = df[df[column] != df[column + '_prev']]
    changes = changes[['spot', column, prefix]]
    return changes

# 计算 PCR POSITION
df_zxt_pcr = calc_position(df_zxt_pcr, open_th=0.8, close_th=0.2, column='pcr')
df_zxt_pcr.to_csv(f'{DATA_DIR}/input/zxt_pcr_position.csv', index=True)
# df_zxt_stock = calc_position(df_zxt_stock, open_th=0.5, close_th=0.5, column='stock')
df_zxt_stock['stock_position'] = df_zxt_stock['stock']
df_zxt_stock.to_csv(f'{DATA_DIR}/input/zxt_stock_position.csv', index=True)

# 保存一份 pcr position changes 的数据
df_zxt_changes = calc_position_changes(df_zxt_pcr, prefix='pcr')
df_zxt_changes.to_csv(f'{DATA_DIR}/input/zxt_pcr_position_changes.csv', index=True)
df_zxt_stock_changes = calc_position_changes(df_zxt_stock, prefix='stock')
df_zxt_stock_changes.to_csv(f'{DATA_DIR}/input/zxt_stock_position_changes.csv', index=True)

# 这个数据是我们自己计算的 PC DIFF 信号
# df_pc = pd.read_csv(f'{DATA_DIR}/input/oi_signal_159915_act_changes.csv')
# df_pc['dt'] = pd.to_datetime(df_pc['dt'])
# df_pc = df_pc.set_index('dt')
# df_pc = df_pc.rename(columns={'action': 'diff_position'})
# df_pc = df_pc[['diff_position']]

# # df join
# df_mask = df_zxt_pcr.join(df_zxt_stock.drop(columns=['spot']), how='outer')
# df_mask = df_mask.join(df_pc, how='outer')
# df_mask['pcr_position'] = df_mask['pcr_position'].ffill().fillna(0)
# df_mask['diff_position'] = df_mask['diff_position'].ffill().fillna(0)
# df_mask['stock_position'] = df_mask['stock_position'].ffill().fillna(0)
# df_mask['mask_position'] = df_mask.apply(lambda row:
#         row['diff_position'] if row['diff_position'] * row['pcr_position'] > 0 else 0, axis=1)
# df_mask['mask2_position'] = df_mask.apply(lambda row:
#         row['diff_position'] if row['diff_position'] * row['stock_position'] > 0 else 0, axis=1)
# df_mask['spot'] = df_mask['spot'].ffill().bfill()
# df_mask = df_mask[['spot', 'mask_position', 'mask2_position', 'diff_position', 'pcr_position', 'stock_position', 'pcr', 'stock']]
# df_mask.to_parquet(f'{DATA_DIR}/input/zxt_mask_position.parquet', engine='pyarrow')
# print(df_mask)

# df_mask['mask_position_prev'] = df_mask['mask_position'].shift(1)
# df_mask['mask_position_prev'] = df_mask['mask_position_prev'].fillna(0)
# df_mask_changes = df_mask[df_mask['mask_position'] != df_mask['mask_position_prev']]
# df_mask_changes = df_mask_changes[['spot', 'mask_position', 'diff_position', 'pcr_position', 'stock_position', 'pcr']]
# print(df_mask_changes)
# df_mask_changes.to_csv(f'{DATA_DIR}/input/zxt_mask_position_changes.csv', index=True)
