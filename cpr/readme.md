# Call Put Ratio  
这里记录这个项目的设计、工作、以及用法。  

这个项目是通过 Call Put Ratio 切片信号进行交易的数据分析项目。  

这个项目的第一期的数据模型如下。  


## 运行方法  

### 简要  
1. 把今年以及过去一年的 Call Put Sum 信息上传到 `data/fact` 文件夹里面。需要回测的部分要带有 ETF 的价格。  
2. 运行 `src/csv2cpr.py` 读取这个文件储存在数据库里。  
3. 运行 SQL 函数 `update_daily` 它会在数据库内部把 call put 计算成 ratio 和 ratio diff 。  
4. 运行脚本 `src/clip.py` 它会读取数据库并且计算每个时间片的分布数据。  
5. 运行脚本 `src/cpr_diff_sig.py` 这是一个高运算量的回测模拟脚本。  
6. 运行脚本 `src/tick2bar.py` 它会读取 `data/fact` 文件并且储存 ETF 价格数据。  
7. 运行 SQL 函数 `update_intraday_spot_clip_profit_range` 它会用 ETF 价格来演算回测模拟的结果，然后得到完整的交易记录。  
8. 统计几千组不同参数的交易记录的脚本在 DBeaver 以及 DadBod 里面，是非常简短的聚合函数。  
9. 统计轮换交易参数的运行结果的脚本在 DadBod Saved Query 里面，也在 `sql/rolling_validate.sql` 里面，这是一个粗糙的轮换函数。  
10. 更加具体的轮换需要运行 roll_run.py 和 roll_merge.py 得到精细的历史策略组合和仓位。其中，  
11. 导出参数和实盘拟真风格的回测。

### 载入数据  
筛选得到新因子的输入需要 Call Oi, Put Oi, Spot Price 三个数据列。  
这三个数据目前是放在一个 CSV 文件里面载入的，这个 CSV 文件使用 Tick 级别的数据。  
有 `call_oi_sum, put_oi_sum, spot_price` 三个数据列。  
<!-- 目前得到这个 CSV 文件的方法是对我的录制数据库执行脚本 `./datavis/oi_scripts/db_oi_csv.py` 。这个脚本之后改造一下可以用来下载实盘数据。   -->  
现在获取实盘数据的脚本更新成了 `./src/dl_oi.py`，这个脚本会下载指定日期范围的历史数据，然后把本地所有的 `call_oi_sum, put_oi_sum, spot_price` csv 文件组合成 `./data/fact/oi_merge/oi_159915.csv` 。运行脚本的参数是 `python dl_oi.py -s 159915 -b 20250818 -e 20250818` 。

修改 `src/csv2cpr.py` 的输入文件然后运行，它会读取 OI 数据储存在 `cpr.cpr` 表格里。  
`cpr.cpr` 的表格数据要转换成每日开盘之后的变化值，建立时间索引方便后续切片的这一步，放在 SQL 函数里面。执行 `select cpr.update_daily('2025-01-01', '2025-02-01', dataset_id, notice=>true);` 可以更新 `cpr.daily` 表格。  

载入计算回测收益需要的 ETF 价格是另一个脚本 `src/tick2bar.py` 这个脚本会把 etf tick price 转换成 minute bar ，然后储存在 `cpr.market_minute` 表格里面。  

### 统计切片和运行回测  

在 `cpr.update_daily` 得到了日内变化量之后，我们需要一个脚本进行日期上的切片统计和分布归一化。这个脚本是 `src/clip.py` 它会按照预设的 30 60 90 天等等进行采样和插值归一化。然后结果储存在 `cpr.clip` 表格里面。  
运行这个采样需要几分钟的时间，这是数据 IO 比较大的运算。  

得到采样之后可以用来计算历史的交易仓位，这个脚本是 `src/cpr_diff_sig.py` 。  
这个脚本输入的时间范围和追加更新数据的时间范围是一致的，它是每日独立测试。  
这是一个多核心高运算量的任务。  

对于轮换之前每个参数的收益情况，用来统计的 SQL 函数是 `select cpr.update_intraday_spot_clip_profit_range(dataset_id, 1, 8082, date_from, date_to, notice=>true);` ，它会读取 `cpr.clip_trade_backtest` 表格，然后把每个参数的逐笔收益储存在 `cpr.clip_trade_profit` 表格里面。  

计算完成之后可以运行 `src/roll_run.py` 得到参数轮换的评估结果，这个脚本读取的时间范围会分成训练数据和测试数据两个时间段落。它会读取 `cpr.clip_trade_profit` 传递给评估函数来给参数排序。评估结果储存在 `cpr.roll_args` `cpr.roll_rank` `cpr.roll_result` 里面。  

`src/roll_merge.py` 会把轮换的参数组合成带有轮换的历史仓位，它读取上一步的 `cpr.roll_result cpr.clip_trade_backtest` 表格，把结果储存在 `cpr.roll_merged` 里面。  

`cpr.roll_merged` 表格里面的历史仓位数据可以用来驱动 nautilus 框架回测，得到更加准确的营收报告。这部分的内容写在 `../backtest/readme.md` 。  

### 实盘信号  

实盘得出信号需要的参数导出脚本是 `src/roll_export.py` ，这个脚本读取表格 `cpr.roll_result` 以及之前回测中用到的切片样本等等数据，在 stdout 输出一个 json 文件。输出的 json 文件也会在数据库里面储存一份。  
实盘运行的 json 文件在数据库里面储存在 `cpr.roll_export` 表格里面，这个表格有一个约束是同一个 `roll_args_id + top_n` 的运行参数的时间窗口不能重叠。这是为了实盘运行的严谨性。  

TODO: **这个约束目前没有生效，不知道是什么原因，可能是 DDL 的版本？**  

时间窗口这样决定 `[dt_from, dt_to)` ，最后一天不包含在内。
实盘运行参数写入数据库按 `cpr.get_or_create_roll_export()` 函数执行。规则是，如果运行的时间窗口完全一样的话，那么 `roll_export.py` 会更新 `cpr.roll_export` 里面储存的实盘运行参数。如果不一样的话会尝试插入，如果时间窗口和之前的数据行没有完全一样的话，那么会尝试插入数据行。插入的约束交给数据库判断，如果有违反约束，例如时间窗口冲突了的话，那么数据库会返回 null id，然后 python 会 raise error 。  


运行这个 json 文件的脚本是 `src/export_run.py` ，脚本会调用 `dl_oi.py` 下载需要的数据，然后直接运行 json 文件的参数得到实时仓位。这个脚本设计成幂等的，指的是在日内的任何时间运行都可以得到当日开盘到当时的完整信号，并且写入数据库，不受之前运行结果的影响。  

实盘信号的实时计算使用 `src/export_run_daemon.py` 这个脚本是一个 wrapper ，默认按开盘时间定期运行 `src/export_run.py` 计算当日的新信号。  

运行这个 export_run / export_run_daemon 脚本的前提需求有几个：  
1. 实盘录制程序将实时市场盘口数据写入数据库，`dl_oi.py` 可以下载到当日交易数据。  
2. daemon 脚本从 CPR 数据库 `cpr.roll_export` 表格中读取当日的交易参数，这个 json 需要存在。

让它开盘中可以定期更新信号写入数据库的运行方法是：  
```bash
while true; do python export_run_daemon.py -s; sleep 3; done
```

让它计算某一天的交易信号，并且写入数据库的的运行方法是：  
```bash
python export_run_daemon.py -d 2025-09-01
```

### 每周更新  

每周更新有一个脚本完成 `src/weekly_update.py` 。这个脚本的工作包括下载数据，载入数据，计算日内变化量，进行采样，运行回测，得到新的轮换参数，然后储存在数据库里面。这个也就是上面所说的整个过程的集成化处理。  

这个脚本让它做回测并且更新下周的运行参数的使用方法如下：  
```bash
python weekly_update.py -s 159915 -b 2025-09-08 -e 2025-09-14  
# -b 输入周一，-e 输入周日，在周五收盘之后运行才能得到可靠的下周轮换结果  
# 这个流程可以处理日常更新的过程，我们需要保证市场数据的完善准确才能得到正确结果。
```

如果只是让它做回测，更新所有参数的 backtest 结果，不触碰 roll 信息，使用方法如下：  
```bash
python weekly_update.py --no-roll -s 159915 -b 2025-09-08 -e 2025-09-14  
# -b -e 随意
```

如果让它做回测，更新 backtest 结果，并且根据当前已有的 roll_result 表格更新 roll_merged 历史信号，使用方法如下：  
```bash
python weekly_update.py --no-roll-next -s 159915 -b 2025-09-08 -e 2025-09-14  
# -b -e 随意
```

如果让它做回测，更新 backtest 结果，更新下一周的 roll_result 和这一周的 roll_merged 历史信号，但是不写入 roll_export 运行参数表格，使用方法如下：
```bash
python weekly_update.py --no-roll-export -s 159915 -b 2025-09-08 -e 2025-09-14  
# -b -e 随意  
# 当 -e 日期是周日的时候它会计算后一周的 roll_result 
```

## 操作清单  
### 当需要换月的时候  
现在都是自动换月，不必人工干预了。设置在 Yaml 文件的 ChainCount 里面。

### 当缺少某天的数据时  
使用 wind 数据库，在启动了 Wind 的 Windows 电脑上运行 `etf_res/datavis/transfer/wind_dl.py` 这是下载期权 Tick 数据的脚本，下载之后更改 `etf_res/datavis/transfer/insert_db.bash` 里面上传数据的日期，这两个文件可以把数据上传到 yuanlan 机器的 postgres 数据库里面。  
然后本地执行 `python export_run_daemon.py -d <date>` 命令，这个命令可以根据那一天的 roll_export 运行参数，读取行情数据库的行情数据并且生成 CPR 信号，如果信号可以正确生成，从开仓到收盘平仓都存在。那么那天的数据是补齐了。  
补齐数据之后可以进行上面流程里面的 backtest / roll / export 等等后续工程。  

### 当需要增加新的 roll method 轮换方案的时候  
在 roll_run.py 里面增加新的方案的描述，注意需要设置独一无二的 name + variation 作为方案的标记名称。  
然后运行总体的 roll_run.py 检查它的输出，输出的表格是 cpr.roll_result 和 cpr.roll_rank 。  
如果这两个都没有问题，然后再手动执行一下 roll_merge.py 得到合成仓位，用这个合成仓位跑一跑回测看看效果是否符合预期。  
之后再执行 roll_export.py 得到可以运行的 json 配置文件。

### 当需要测试信号效果的时候  
cpr roll 的结果是储存在 cpr.roll_merged 表格里面，这个表格的数据是 cpr_merge.py 脚本计算的。这个 cpr_merge.py 脚本需要的输入是当时的 cpr.roll_result 轮换排序和 cpr.clip_trade_backtest 仓位。  
使用 ./src/update_roll_csv.py 脚本可以从 cpr.roll_merged 表格里面到处数据保存在 csv 文件里面。  

## 数据可视化  
作为我们估计 CPR 指标的准确性，我们需要绘制 CPR 在单日之内的变化，还有触发阈值的四条线。  
我们也许可以从触发阈值的图形上面发现一些问题。  
因为现在的问题可能出在两个地方，一个是触发位置，另一个是 CPR 和日内股价的脱离问题。除去这两点之外的话，剩下的问题是 CPR 的上下跳动引起的抗单。  
触发有触发线，触发线的自适应问题是所有问题的核心。  
ZScore 或者其它的切片方法的核心是在触发线的选择上有不同的理解。我们现在的挑选模式是中间完全黑盒，完全结果导向。  
有没有一种形式可以解释其中的统计偏差和因果关系。  

根据 2026 年初的上下跳变的情况，看起来只有 Per Strike CPR 可以清洗这种迷雾。累加的迷雾是问题。  


## TODO
这里 python 脚本里面有一个问题就是 to_sql 函数里面滥用 upsert_on_conflict_skip ，我都不知道一个脚本最后执行有没有写入数据库，完全就是随缘的。至少有下面几个高危使用位置需要更换：  

1. json run 脚本 export_run.py 写入仓位信号的函数 `save_roll_export_run` .  
2. roll 轮换选择参数的脚本 roll.py 写入轮换结果的函数 `save_roll_output` .  
3. 合并轮换的历史仓位的脚本 roll_merge.py 写入合并结果的函数。  

滥用的原因来自于很多更新函数的写入都是有重叠的，其实应该是检查和之前的写入数值是否一致，有些允许不一致的时候更新，有些就应该在不一致的时候报错。  

有些表格不能直接覆盖插入，例如说 roll_export, roll_result, roll_merged 这些表格都应该手动备份之后，清除之前的表格数据，然后去执行。


TODO: 关于回测结果，还需要一个安全检查的脚本，检查目前这些 clip_trade_profit 的数据行的开仓时间插口之间有没有重叠的情况。  



