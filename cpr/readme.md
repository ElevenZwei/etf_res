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
目前得到这个 CSV 文件的方法是对我的录制数据库执行脚本 `./datavis/oi_scripts/db_oi_csv.py` 。这个脚本之后改造一下可以用来下载实盘数据。  

修改 `src/csv2cpr.py` 的输入文件然后运行，它会读取 OI 数据储存在 `cpr.cpr` 表格里。  
`cpr.cpr` 的表格数据要转换成每日开盘之后的变化值，建立时间索引方便后续切片的这一步，放在 SQL 函数里面。执行 `select cpr.update_daily('2025-01-01', '2025-02-01', dataset_id, notice=>true);` 可以更新 `cpr.daily` 表格。  

载入计算回测收益需要的 ETF 价格是另一个脚本 `src/tick2bar.py` 这个脚本会把 etf tick price 转换成 minute bar ，然后储存在 `cpr.market_minute` 表格里面。  

### 统计切片和运行回测  
在 `cpr.update_daily` 得到了日内变化量之后，我们需要一个脚本进行日期上的切片统计和分布归一化。这个脚本是 `src/clip.py` 它会按照预设的 30 60 90 天等等进行采样和插值归一化。然后结果储存在 `cpr.clip` 表格里面。  
得到采样之后可以用来计算历史的交易仓位，





