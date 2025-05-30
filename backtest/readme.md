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

## 输入数据说明
输入的主要文件是 tl_greeks_159915_all_fixed.csv 文件。
这个文件里面包含了 2024 年的创业板期权数据。
文件名结尾有 fixed 表示它的价格是 close price 。
否则通联的原始数据价格是 open price 。

输入的逻辑代码在 data_types.py 里面，这里面有它会去解读的数据列。

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


