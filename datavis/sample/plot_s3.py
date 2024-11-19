import numpy as np
import pandas as pd
import plotly.graph_objects as go
from scipy.interpolate import griddata

# 假设这是原始的较低分辨率的数据
data = {
    'X': np.random.uniform(-5, 5, 100),
    'Y': np.random.uniform(-5, 5, 100),
    'Z': np.sin(np.sqrt(np.random.uniform(-5, 5, 100)**2 + np.random.uniform(-5, 5, 100)**2))
}
df = pd.DataFrame(data)

# 生成高分辨率的插值网格
x_high_res = np.linspace(df['X'].min(), df['X'].max(), 200)
y_high_res = np.linspace(df['Y'].min(), df['Y'].max(), 200)
X_high_res, Y_high_res = np.meshgrid(x_high_res, y_high_res)

print(X_high_res)
print(Y_high_res)
# 使用 griddata 进行插值
Z_high_res = griddata(
    points=(df['X'], df['Y']),
    values=df['Z'],
    xi=(X_high_res, Y_high_res),
    method='cubic'  # 使用 cubic（三次样条）插值，效果更平滑
)

# 创建 Plotly 图形，使用插值后的数据绘制曲面
fig = go.Figure(data=[go.Surface(
    x=X_high_res,
    y=Y_high_res,
    z=Z_high_res,
    contours={
        "z": {
            "show": True,
            "color": "black",
            "width": 2,
            "usecolormap": True,
            "highlightcolor": "lime",
            "highlightwidth": 4
        }
    }
)])

# 设置图形布局
fig.update_layout(
    title='High-Resolution Surface with Interpolated Data',
    scene=dict(
        xaxis_title='X Axis',
        yaxis_title='Y Axis',
        zaxis_title='Z Axis'
    )
)

# 显示图形
fig.show()