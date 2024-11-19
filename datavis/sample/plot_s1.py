import plotly.graph_objects as go
import numpy as np

# 生成三维曲面的 X, Y, Z 数据
x = np.linspace(-5, 5, 100)
y = np.linspace(-5, 5, 100)
X, Y = np.meshgrid(x, y)
Z = np.sin(np.sqrt(X**2 + Y**2))  # 这里是一个简单的波浪曲面

# 创建三维曲面
print(X)
print(Y)
print(Z)
surface = go.Surface(x=X, y=Y, z=Z, colorscale='Viridis', opacity=0.8)

# 生成 XY 平面上的曲线（例如一条抛物线）
x_line = np.linspace(-5, 5, 100)
y_line = 0.5 * x_line**2  # 这里是一条抛物线
z_line = np.zeros_like(x_line)  # Z 轴为 0，代表在 XY 平面

# 在 XY 平面绘制曲线
curve = go.Scatter3d(x=x_line, y=y_line, z=z_line, mode='lines', line=dict(color='red', width=5))

# 将曲线拉伸成垂直于 XY 平面的曲面
# 重复曲线数据，通过 Z 轴拉伸
z_stretch = np.linspace(0, 10, 50)
x_stretch, z_stretch = np.meshgrid(x_line, z_stretch)
y_stretch = np.tile(y_line, (50, 1))

# 绘制垂直于 XY 平面的拉伸曲面
stretched_surface = go.Surface(x=x_stretch, y=y_stretch, z=z_stretch, colorscale='Reds', opacity=0.6)

# 创建布局
layout = go.Layout(
    scene=dict(
        xaxis_title='X Axis',
        yaxis_title='Y Axis',
        zaxis_title='Z Axis'
    ),
    title="3D Surface with XY Plane Curve and Stretched Surface"
)

# 合并图形
fig = go.Figure(data=[surface, curve, stretched_surface], layout=layout)

# 显示图形
fig.show()