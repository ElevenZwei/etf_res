# Call Put Ratio  
这里记录这个项目的设计、工作、以及用法。  

这个项目是通过 Call Put Ratio 切片信号进行交易的数据分析项目。  

这个项目的第一期的数据模型如下。  


## 运行方法  
1. 把今年以及过去一年的 Call Put Sum 信息上传到 `data/fact` 文件夹里面。需要回测的部分要带有 ETF 的价格。  
2. 运行 `src/csv2cpr.py` 读取这个文件储存在数据库里。  
3. 运行函数 `update_daily` 它会在数据库内部把 call put 计算成 ratio 和 ratio diff 。  
4. 运行脚本 `src/clip.py` 它会读取数据库并且计算每个时间片的分布数据。  
5. 运行脚本 `src/cpr_diff_sig.py` 这是一个高运算量的回测模拟脚本。  
6. 运行脚本 `src/tick2bar.py` 它会读取 `data/fact` 文件并且储存 ETF 价格数据。  
7. 运行函数 `update_intraday_spot_clip_profit_range` 它会用 ETF 价格来演算回测模拟的结果，然后得到完整的交易记录。  
8. 统计几千组不同参数的交易记录的脚本在 DBeaver 以及 DadBod 里面，是非常简短的聚合函数。


## TODO  
这个项目里面目前还有一些需要修正的问题。  
1. `get_or_create` 系列的函数需要先 select 一次，不然制造的 id 数字太过于巨大了。  
2. 修正现有的过于庞大的 id 数字，把它们更新成 max + 1 。  



