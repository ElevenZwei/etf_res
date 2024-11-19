import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import butter, filtfilt
import scipy.signal as ssig

# 创建示例信号（带噪声的正弦波）
fs = 500  # 采样频率
t = np.arange(0, 1, 1/fs)  # 时间向量
signal = np.sin(2 * np.pi * 5 * t) + np.random.normal(0, 0.5, t.shape)

def left_gaus(sig, wsize, sigma):
    gaus = ssig.windows.gaussian(wsize, sigma)
    gaus[:wsize // 2] = 0
    gaus /= np.sum(gaus)
    return np.convolve(sig, gaus, mode='same')

def full_gaus_delayed(sig, wsize, sigma):
    gaus = ssig.windows.gaussian(wsize, sigma)
    gaus /= np.sum(gaus)
    gaus = np.concat([np.zeros_like(gaus), gaus])
    return np.convolve(sig, gaus, mode='same')

# 设计 Butterworth 低通滤波器
cutoff_freq = 10  # 截止频率
order = 4  # 滤波器阶数
b, a = butter(order, cutoff_freq / (0.5 * fs), btype='low')

# 应用滤波器
filtered_signal = filtfilt(b, a, signal)
left_gaus_signal = left_gaus(signal, 100, 30)
full_gaus_signal = full_gaus_delayed(signal, 100, 10)

# 绘制原始信号和平滑后的信号
plt.figure(figsize=(10, 5))
plt.plot(t, signal, label='原始信号', alpha=0.5)
plt.plot(t, filtered_signal, label='Butterworth 滤波后的信号', color='red')
plt.plot(t, left_gaus_signal, label='Left Gaussian 滤波后的信号')
plt.plot(t, full_gaus_signal, label='Full Gaussian With Delay 滤波后的信号')
plt.legend()
plt.title('Butterworth 滤波器示例')
plt.xlabel('时间 (秒)')
plt.ylabel('幅度')
plt.grid()
plt.show()