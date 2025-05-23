import pandas as pd
import numpy as np

# 创建示例 DataFrame
df = pd.DataFrame({
    'a': [1, 2, 3],
    'b': [4, 5, 6],
    'c': [7, 8, 9],
    'd': [10, 11, 12]
})

# 获取 DataFrame 的列名
columns = df.columns

# 定义填充函数，填充每行的两侧并滑动窗口
def sliding_window_with_padding(df, winsize):
    result = []
    pad_size = (winsize - winsize % 2) // 2
    # 这里先转置 DataFrame，然后对每一列进行填充
    df_t = df.transpose()
    for colname in df_t.columns:
        # 对原先的每一行左右进行填充
        col = df_t[colname]
        padded_row = np.pad(col.values, (pad_size, pad_size), mode='edge')
        # 滑动窗口的方式获取原先 N 列对应的数组
        result.append([padded_row[i:i+winsize] for i in range(len(col))])
    return pd.DataFrame(result, columns=df.columns)

def strike_pivot_id_grid(strike_grid: pd.DataFrame):
    """对于一个 strike grid，生成一个编号的 pivot grid"""
    pivot_grid = pd.DataFrame(
            np.tile(np.arange(len(strike_grid.columns)), (len(strike_grid), 1)),
            columns=strike_grid.columns, index=strike_grid.index)
    return pivot_grid

# 调用函数处理 DataFrame
# result = sliding_window_with_padding(df, winsize=3)

# 打印结果
# print(result)
# print(result.iloc[0, 3].shape)
# print(strike_pivot_id_grid(result))


# from scipy import stats
# def spearmanrho():
#     a = np.array([0.5, 0.2, 0.4, 0.1])
#     # b = np.arange(len(a))
#     b = np.sort(a)
#     rho, pval = stats.spearmanr(a, b)
#     print(rho, pval)

# spearmanrho()

## 测试一下之前数据的填充和均匀问题

# def mdt_count(df: pd.DataFrame):
#     count = df.groupby('dt').count()
#     count = count[['strike']]
#     count['cnt_avg'] = count['strike'].expanding().mean()
#     print(count)

# mdt_count(pd.read_csv('..\data\dsp_input\strike_oi_diff_159915_exp20241127_date20241125.csv'))
# mdt_count(pd.read_csv('..\data\dsp_input\strike_oi_diff_159915_exp20250122_date20250120.csv'))

import numpy as np
import pandas as pd
import scipy.interpolate as spi

# 示例 DataFrame，每一行是一个需要插值的序列
df = pd.DataFrame({
    'A': [0, 1, 4, 9, 16, 25],
    'B': [0, 2, 8, 18, 32, 50],
    # 'B': [3510.6, 3503.3, 3497.7, 3481.7, 3483.5, 3484.5],
    'C': [1, 3, 6, 10, 15, 21]
})

# # 目标插值后的长度
# factor = 3  # 增加 3 倍密度
# new_len = df.shape[1] * factor

# def cubic_spline_interpolation(row):
#     """对 DataFrame 的一行数据进行 Cubic Spline 插值"""
#     x = np.linspace(0, len(row) - 1, len(row))  # 原始 x 轴索引
#     x_new = np.linspace(0, len(row) - 1, new_len)  # 新 x 轴索引
#     cs = spi.CubicSpline(x, row)  # 计算三次样条插值
#     return cs(x_new)  # 计算新 y 值

# print(df)

# print(df.apply(cubic_spline_interpolation, axis=1))
# # 对 DataFrame 每一行进行插值，并转换为新的 DataFrame
# df_interpolated = pd.DataFrame(np.vstack(df.apply(cubic_spline_interpolation, axis=1)))

# # 打印结果
# print(df_interpolated)

df['x'] = df['B'].expanding().mean()
df['y'] = df['B'].expanding().std()
print(df)