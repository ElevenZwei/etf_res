import pandas as pd
import numpy as np

# 创建示例数据
data = {
    'x': [0, 0, 1, 1, 2, 2],
    'y': [0, 1, 0, 1, 0, 1],
    'z': [1, 2, 3, 4, 5, 6]
}

df = pd.DataFrame(data)

# 使用 pivot 操作创建二维数据表
pivot_df = df.pivot(index='y', columns='x', values='z')

print("Pivot DataFrame:")
print(pivot_df)

from scipy.interpolate import griddata

# 提取插值所需的数据
x = pivot_df.columns.values  # x轴
y = pivot_df.index.values     # y轴
z = pivot_df.values.flatten()  # z轴数据，扁平化为一维数组
x_grid, y_grid = np.meshgrid(x, y)
x_flat = x_grid.flatten()
y_flat = y_grid.flatten()

# 创建新的网格以进行插值
xi = np.linspace(x.min(), x.max(), 100)  # 生成新的 x 坐标
yi = np.linspace(y.min(), y.max(), 100)  # 生成新的 y 坐标
xi_grid, yi_grid = np.meshgrid(xi, yi)               # 创建网格

# 使用 griddata 进行立方插值
zi = griddata((x_flat, y_flat), z, (xi_grid, yi_grid), method='cubic')
inter_df = pd.DataFrame(zi, columns=xi, index=yi)

print("\nInterpolated Values (zi):")
print(inter_df)