"""
这个脚本的作用是：
对 ZXT 提供的 PCR 信号，做一个 PC DIFF 固定阈值的遮罩裁剪，
把新的信号储存起来。
"""

import pandas as pd
from backtest.config import DATA_DIR

# 这个数据是 ZXT 提供的 PCR 信号
df_zxt = pd.read_parquet(f'{DATA_DIR}/zxt/pc_final_pos.parquet', engine='pyarrow')
df_zxt = df_zxt.rename(columns={'159915': 'pcr'})
df_zxt = df_zxt.rename_axis('dt')
df_zxt.index = df_zxt.index.tz_localize('Asia/Shanghai')
df_zxt['spot'] = '159915'

# 这里还要再加一步，从 PCR 到 PCR ACTION，
# 例如 PCR > 0.8 的时候开仓，在 PCR < 0.2 的时候平仓
class PCRAction:
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

pcr_action = PCRAction(open_th=0.8, close_th=0.2)
df_zxt['pcr_action'] = df_zxt['pcr'].apply(pcr_action)
df_zxt.to_csv(f'{DATA_DIR}/input/zxt_pcr_action.csv', index=True)

# 保存一份 pcr action changes 的数据
df_zxt['pcr_action_prev'] = df_zxt['pcr_action'].shift(1)
df_zxt['pcr_action_prev'] = df_zxt['pcr_action_prev'].fillna(0)
df_zxt_changes = df_zxt[df_zxt['pcr_action'] != df_zxt['pcr_action_prev']]
df_zxt_changes = df_zxt_changes[['spot', 'pcr_action', 'pcr']]
df_zxt_changes.to_csv(f'{DATA_DIR}/input/zxt_pcr_action_changes.csv', index=True)

# 这个数据是我们自己计算的 PC DIFF 信号
df_pc = pd.read_csv(f'{DATA_DIR}/input/oi_signal_159915_act_changes.csv')
df_pc['dt'] = pd.to_datetime(df_pc['dt'])
df_pc = df_pc.set_index('dt')
df_pc = df_pc.rename(columns={'action': 'diff_action'})
df_pc = df_pc[['diff_action']]

# df join
df_mask = df_zxt.join(df_pc, how='outer')
df_mask['pcr_action'] = df_mask['pcr_action'].ffill().fillna(0)
df_mask['diff_action'] = df_mask['diff_action'].ffill().fillna(0)
df_mask['mask_action'] = df_mask.apply(lambda row:
        row['pcr_action'] if row['diff_action'] * row['pcr_action'] > 0 else 0, axis=1)
df_mask['spot'] = df_mask['spot'].ffill().bfill()
df_mask.to_parquet(f'{DATA_DIR}/input/zxt_mask_action.parquet', engine='pyarrow')
print(df_mask)

df_mask['mask_action_prev'] = df_mask['mask_action'].shift(1)
df_mask['mask_action_prev'] = df_mask['mask_action_prev'].fillna(0)
df_mask_changes = df_mask[df_mask['mask_action'] != df_mask['mask_action_prev']]
df_mask_changes = df_mask_changes[['spot', 'mask_action', 'diff_action', 'pcr_action', 'pcr']]
print(df_mask_changes)
df_mask_changes.to_csv(f'{DATA_DIR}/input/zxt_mask_action_changes.csv', index=True)
