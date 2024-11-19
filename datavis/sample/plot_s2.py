import plotly.graph_objects as go
import numpy as np
import pandas as pd

# 生成时间数据，假设是每天的时间点
time = pd.date_range(start='2024-01-01', periods=100, freq='D')

# 转换为数值形式（可选，如果需要处理数值计算）
time_numeric = time.astype(int) / 10**9  # 转换为 UNIX 时间戳，秒为单位

# 假设 Y 轴是某种价格数据，Z 轴是另一个与价格相关的变量
y = np.linspace(0, 100, 100)
X, Y = np.meshgrid(time_numeric, y)
Z = np.sin(X / 1e8) * np.cos(Y / 10)  # 生成一个基于时间和价格的曲面

# 创建三维曲面
# print(X)
# print(Y)
# print(Z)
surface = go.Surface(x=X, y=Y, z=Z, colorscale='Viridis', opacity=0.8)

# 创建在 XY 平面上的曲线，X 是时间，Y 是价格曲线
price_curve = np.sin(time_numeric / 1e6) * 50  # 一个价格曲线示例
curve = go.Scatter3d(x=time, y=price_curve, z=np.zeros_like(price_curve),
                     mode='lines', line=dict(color='red', width=5))

# 将曲线拉伸到 Z 轴
z_stretch = np.linspace(-2, 2, 50)
time_stretch, z_stretch = np.meshgrid(time_numeric, z_stretch)
price_stretch = np.tile(price_curve, (50, 1))

# 绘制拉伸的曲面
stretched_surface = go.Surface(x=time_stretch, y=price_stretch, z=z_stretch, colorscale='Reds', opacity=0.6)

# 创建布局
layout = go.Layout(
    scene=dict(
        xaxis_title='Time',
        yaxis_title='Price',
        zaxis_title='Z Axis',
    ),
    title="3D Surface with Time Axis and Stretched Curve"
)

# 合并图形
fig = go.Figure(data=[surface, curve, stretched_surface], layout=layout)

# 显示图形
fig.show()