"""
这个文件提供一些辅助函数，用于数据处理和可视化。
主要是带状态的的信号处理。
"""

class OpenCloseHelper:
    def __init__(self, long_open, long_close, short_open, short_close):
        self.long_open = long_open
        self.long_close = long_close
        self.short_open = short_open
        self.short_close = short_close
        self.state = 0
    
    def next(self, value):
        if value > self.long_open:
            self.state = 1
        elif value < self.short_open:
            self.state = -1
        if self.state == 1 and value < self.long_close:
            self.state = 0
        if self.state == -1 and value > self.short_close:
            self.state = 0
        return self.state
    
    def last(self):
        return self.state


class DiffHelper:
    def __init__(self):
        self.state = 0
        self.last = 0
    
    def next(self, value):
        self.last = value - self.state
        self.state = value
        return self.last
    
    def last(self):
        return self.last


class BasePriceOpenHelper:
    """
    在可交易的时间单纯做多，用于统计基础价格的变动。
    """
    def config(self, conf):
        pass

    def next(self, ts, sigma, spot, dt):
        return 1


class TsOpenHelper:
    def __init__(self):
        pass

    def config(self, conf):
        self.helper = OpenCloseHelper(
            conf['ts_open'], conf['ts_close'],
            -conf['ts_open'], -conf['ts_close'],
        )
    
    def next(self, ts, sigma, spot, dt):
        return self.helper.next(ts)


class SigmaOpenHelper:
    def __init__(self):
        pass

    def config(self, conf):
        self.helper = OpenCloseHelper(
            conf['sigma_open'], conf['sigma_close'],
            -conf['sigma_open'], -conf['sigma_close'],
        )
    
    def next(self, ts, sigma, spot, dt):
        return self.helper.next(sigma)

class TsSigmaOpenHelper:
    def __init__(self):
        pass

    def config(self, conf):
        self.ts_helper = OpenCloseHelper(
            conf['ts_open'], conf['ts_close'],
            -conf['ts_open'], -conf['ts_close'],
        )
        self.sigma_helper = OpenCloseHelper(
            conf['sigma_open'], conf['sigma_close'],
            -conf['sigma_open'], -conf['sigma_close'],
        )
    
    def next(self, ts, sigma, spot, dt):
        ts_pos = self.ts_helper.next(ts)
        sigma_pos = self.sigma_helper.next(sigma)
        if ts_pos == sigma_pos:
            return ts_pos
        else:
            return 0


class TsOpenSigmaCloseHelper:
    """
    组合一下 Ts Open 的快速开仓和 Sigma Close 在躲避大反弹的时候平仓。
    """
    def __init__(self):
        # state
        self.ts_state = 0
        self.state = 0
        self.spot_max = -10000
        self.spot_min = 10000
    
    def config(self, conf):
        self.ts_open = conf['ts_open']
        self.ts_close = conf['ts_close']
        self.sigma_close = conf['sigma_close']
        self.sigma_open = self.sigma_close + 80
        self.stop_loss = conf['p2p_stop_loss']
    
    def next(self, ts, sigma, spot, dt):
        if self.state == 0:
            if self.ts_state == 0 and ts > self.ts_open and sigma > self.sigma_open:
                self.ts_state = 1
                self.state = 1
            elif self.ts_state == 0 and ts < -1 * self.ts_open and sigma < -1 * self.sigma_open:
                self.ts_state = -1
                self.state = -1
            elif self.ts_state == 1 and ts < self.ts_close:
                # print('ts reset from 1')
                self.ts_state = 0
            elif self.ts_state == -1 and ts > -1 * self.ts_close:
                # print('ts reset from -1')
                self.ts_state = 0

        elif self.state == 1:
            if ts < self.ts_close:
                self.ts_state = 0
                self.state = 0
            elif sigma < self.sigma_close:
                self.state = 0
        elif self.state == -1:
            if ts > -1 * self.ts_close:
                self.ts_state = 0
                self.state = 0
            elif sigma > -1 * self.sigma_close:
                self.state = 0

        if self.state != 0:
            self.spot_max = max(self.spot_max, spot)
            self.spot_min = min(self.spot_min, spot)
            if self.spot_max > 0 and self.spot_min > 0:
                if (self.state == 1
                        and spot / self.spot_max - 1 < -1 * self.stop_loss):
                    self.state = 0
                elif (self.state == -1
                        and spot / self.spot_min - 1 > self.stop_loss):
                    self.state = 0
        else:
            self.spot_max = -10000
            self.spot_min = 10000
        
        return self.state


class TsOpenSigmaCloseTwoStepHelper:
    """根据 TS 点阵图里面的分布特性，
    发现在一轮收益达到 1% 以上的时候可以适当放宽止损的条件，在收益小的时候需要尽快止盈止损。
    我需要在每天的行情里可以区分慢牛和突刺，
    但是这个操作和突刺收益平仓的操作正好完全相反，为了解释这个问题，
    我们可以在点阵图上分割出一轮收益 1% 以上然后出现下潜的对象有多少可能再次上升到 1% """
    def __init__(self, ts_open, ts_close, sigma_close_1, sigma_close_2, step_size):
        pass


class TsOpenSigmaReopenHelper:
    """ 这个的灵感是来自于发现 Sigma Close 之后根据 Sigma 的反弹还会有不错的盈利机会得到。"""
    def __init__(self):
        self.state = 0
    
    def config(self, conf):
        self.ts_open = conf['ts_open']
        self.ts_close = conf['ts_close']
        self.sigma_open = conf['sigma_open']
        self.sigma_close = conf['sigma_close']
    
    def next(self, ts, sigma, spot, dt):
        if self.state == 0:
            if ts > self.ts_open and sigma > self.sigma_open:
                self.state = 1
            elif ts < -1 * self.ts_open and sigma < -1 * self.sigma_open:
                self.state = -1
        elif self.state == 1:
            if ts < self.ts_close:
                self.state = 0
            elif sigma < self.sigma_close:
                self.state = 0
        elif self.state == -1:
            if ts > -1 * self.ts_close:
                self.state = 0
            elif sigma > -1 * self.sigma_close:
                self.state = 0
        return self.state


class TsOpenTakeProfitHelper:
    """
    Ts Open 的快速开仓和 Take Profit 在高点回落的时候及时止盈平仓。
    """
    def __init__(self):
        self.ts_state = 0
        self.state = 0
        self.spot_min = 100000
        self.spot_max = -100000
    
    def config(self, conf):
        self.ts_open = conf['ts_open']
        self.ts_close = conf['ts_close']
        self.stop_loss = conf['stop_loss']
    
    def next(self, ts, sigma, spot_price, dt):
        # 这个 if 让它开仓之后在选择记录最高点和最低点（这一点带来的影响不太清楚）。
        # 就是说如果没有这个 if ，当前面已经有非常高的高峰，那么这个开仓信号就会被忽略。
        if self.state == 0:
            self.spot_min = 100000
            self.spot_max = -100000
        else:
            self.spot_min = min(self.spot_min, spot_price)
            self.spot_max = max(self.spot_max, spot_price)
        if self.state == 0:
            if self.ts_state == 0 and ts > self.ts_open:
                self.ts_state = 1
                self.state = 1
            elif self.ts_state == 0 and ts < -1 * self.ts_open:
                self.ts_state = -1
                self.state = -1
        elif self.state == 1:
            if ts < self.ts_close:
                self.ts_state = 0
                self.state = 0
            elif spot_price / self.spot_max < 1 - self.stop_loss:
                self.state = 0
        elif self.state == -1:
            if ts > -1 * self.ts_close:
                self.ts_state = 0
                self.state = 0
            elif spot_price / self.spot_min > 1 + self.stop_loss:
                self.state = 0
        return self.state

# 单纯的 stop loss 给出了和 ts 差不多的成绩，但是亏损可控了。
# 这里可以再深入加强一下，改成抗单 1% 但是如果盈利超过 1% 那么在回落 0.3% 的时候止盈
# 可能这个回落 0.3 也有点太少了。
# 是否可以在盈利超过一定程度的时候接受来自 sigma 的平仓信号，否则选择回落 1% 的止盈。
# Sigma 平仓信号目测的损失还不如回落止盈。

# 更加科学的统计方法是绘制一个，每一笔开仓过程里最大盈利和最大亏损和平仓受益的相关性点阵图。
# 这个应该用 signal + count not zero 的方法，然后标记出每一个仓位状态下的编号，然后再用 group by 聚合。
