# 这个是原始版本的绘图脚本，绘制一个经过了插值之后的 OI 曲面以及 spot 在其中游走的样子。
import plotly.graph_objects as go
import numpy as np
import pandas as pd
from scipy.interpolate import griddata

df = pd.read_csv('dsp_input/strike_oi_diff_159915_20241114.csv')
df = df.loc[(df['strike'] > 1.4) & (df['strike'] < 2.9)]
# df.set_index('')

df = df[['dt', 'strike', 'oi_diff_c', 'oi_diff_p', 'spot_price']]
df['oi_diff_cp'] = df['oi_diff_c'] - df['oi_diff_p']
df_xyz = df[['dt', 'strike', 'oi_diff_cp']]
df_spot = df[['dt', 'spot_price']].drop_duplicates()

# print(df_xyz)
x_uni = np.sort(df['dt'].unique())
y_uni = np.sort(df['strike'].unique())
x_grid, y_grid = np.meshgrid(x_uni, y_uni)
z_grid = df_xyz.pivot(index='strike', columns='dt', values='oi_diff_cp').values
zero_grid = np.zeros_like(x_grid)

# 插值采样
# 这个东西的数据点太多了，我们需要先做一个 EMA 归一化。
# 这个函数后来发现有未来函数通过插值影响过去数据的问题，所以后来改成了一维插值。
x_uni_epoch = pd.to_datetime(x_uni).astype('int64') / 1e12 - 1.72e6
print(x_uni_epoch)
y_high_res_lin = np.linspace(y_uni[0], y_uni[-1], 200)
x_high_res_epoch, y_high_res_grid = np.meshgrid(x_uni_epoch, y_high_res_lin)
dt_epoch = pd.to_datetime(df['dt']).astype('int64') / 1e12 - 1.72e6
z_high_res = griddata(
    points=(dt_epoch, df['strike']),
    values=df['oi_diff_cp'],
    xi=(x_high_res_epoch, y_high_res_grid),
    method='cubic'
)
x_high_res_grid, y_high_res_grid = np.meshgrid(x_uni, y_high_res_lin)

oi_surf = go.Surface(x=x_high_res_grid, y=y_high_res_grid, z=z_high_res,
        colorscale='Viridis', opacity=0.9,
        contours={
                "x": {"show": True, "color": "#222"},  # 显示 XZ 平面的切割线
                # "y": {"show": True, "color": "blue"},  # 显示 YZ 平面的切割线
                # "z": {"show": False}  # 隐藏 XY 平面的切割线
        },
)

zero_surf = go.Surface(x=x_grid, y=y_grid, z=zero_grid,
        opacity=0.2,
)

x_sp = df_spot['dt']
y_sp = df_spot['spot_price']
curve_sp = go.Scatter3d(x=x_sp, y=y_sp, z=np.zeros_like(x_sp) + 4000,
                        mode='lines', line=dict(color='red', width=5))
z_sp = np.linspace(-4000, 4000, 50)
x_sp_st, z_sp_st = np.meshgrid(x_sp, z_sp)
y_sp_st = np.tile(y_sp, (50, 1))
sp_st = go.Surface(x=x_sp_st, y=y_sp_st, z=z_sp_st, colorscale='Reds', opacity=0.2)

layout = go.Layout(
    scene=dict(
        xaxis_title='Time',
        yaxis_title='Strike',
        zaxis_title='OiDiff',
    ),
    title="159915 Strike OiDiff"
)

fig = go.Figure(data=[oi_surf, zero_surf, curve_sp, sp_st], layout=layout)
fig.show()
