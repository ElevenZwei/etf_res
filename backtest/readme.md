# 整理方案
过去的代码只要不写说明就会变成乱七八糟的垃圾代码。
然后自己也不知道怎么整理怎么清理。
所以说一个可以长期持续的生存方案需要很多部分。
比如说输入部分的阶段流程化处理，
还有输出部分的流程化处理，不能全都混在一个文件夹里面了。

## 运行方法
`python -m backtest.nautilus.backtest_xxx <args>`  
运行之后会产生三个 csv 文件，输出在 `backtest/data/output` 位置。  
分别是 `account / order / pos` 三个 csv 文件。  

对于运行之后产品的后期处理，现在有 log 处理和 csv 处理两个路线。  
首推 csv 处理的路线，在有了三个 csv 文件之后运行两个脚本。  
`python -m backtest.nautilus.afx.afx_order_df -f <order.csv>`  
`python -m backtest.nautilus.afx.afx_order_2_worth`  
第一个脚本整理 nautilus 回测框架输出的 order csv 文件内容。
第二个脚本读取标的价格和订单记录合成账户的净值变化。

## 策略说明
这套系统当时做出来的时候就非常赶时间，所以很多地方都是补丁和奇怪的 IF 。
比如说 strategy_etf strategy_buy 可以接受小数范围的仓位变动，但是 strategy_sell 只有整个开整个平的功能。
代码里面的 size_mode 同时肩负了只做多，只做空，每日平仓还是不平仓的功能。


## 输入数据说明
2024 年数据的主要文件是 tl_greeks_159915_all_fixed.csv 文件。  
这个文件里面包含了 2024 年的创业板期权数据。
文件名结尾有 fixed 表示它的价格是 close price 。
否则通联的原始数据价格是 open price 。  

2025 年数据输入的文件是 `input/opt_159915_2025_*.csv`  
计算 Greeks 的脚本是 `159915_2025.py` 。  
我们现在应该已经有到 0709 为止的数据，那么我们用合成地方式处理一下。  
0709 到 0815 这段先放一下。


输入解析的逻辑代码在 data_types.py 里面，这里面有它会去解读的数据列。

# 2025-08-19 CPR 系统的信号回测
我们主要考虑追加数据过程里面的自动处理方法。
这有两个方面需要设计，一个是怎么自动收集计算所有的 Greeks 。
另一个方面是搞一个新的表格能够直接读取数据，不要总是 CSV 中转了。


# 2025-05-21 郑心棠的信号回测

## 需求和设计
James 目前的想法是，在她的信号和我们的固定值信号出现重叠的时候进行交易。

我打算在外合成信号，然后在 nautilus 里面做几个根据信号产生期权持仓的回测代码。
这样这个回测代码之后是各种信号都可以通用的，对减轻我的负担大有好处。

考虑到郑心棠的信号有仓位大小，所以我用 4000 固定阈值在郑心棠的信号上做一个 mask 。  
4000 固定阈值的信号是 oi_signal_159915_act_changes.csv 。  
导入这个 csv 之后，用 pandas join 一下两个 df ，然后 ffill ，再做一下 mask。  
这样就得到了重叠的信号，然后再用这个重叠的信号，输入 nautilus 去做回测。

所以说，我们如果想要做固定阈值的信号回测，需要读取的是  
`oi_signal_159915_act_changes.csv`  
如果需要做 zxt pcr 原始信号的回测，我们需要读取  
`zxt_pcr_action_changes.csv`  
如果需要做综合信号的回测，我们需要读取的是  
`zxt_mask_action_changes.csv`

现在信号文件我都得到了，下一步是编写读取标的价格和信号的 Nautilus 策略。
价格数据在 `input/options_data.csv` 里面，  
这个文件里面有 2024 年 1 月 - 10 月 23 日的 159915 的所有期权价格数据。

按照我之前的买权策略 `strategy_buy.py` 的要求，
这个策略文件需要把 Signal 和 Spot 行情组合在一起。


