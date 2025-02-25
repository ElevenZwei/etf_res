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
    
    def value(self):
        return self.state

    