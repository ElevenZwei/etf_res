"""
对于一段时间，用每周一作为起始点训练之前的数据，然后预测未来一周的值，进行交易。
"""

import click
import os
import datetime
import pandas as pd
import numpy as np
import glob

import linear_reg_1 as s1
import reg_trade_1 as s2

def concat_trades(dts: list[str]):
    """
    将所有交易结果合并到一个文件中。
    :param dts: 日期列表
    """
    st_dfs = {} # strategy -> list[dataframe]
    for dt in dts:
        fs = glob.glob(f'../output/{dt}/reg*_trades_validate.csv')
        for f in fs:
            # use pathlib to get the strategy name correctly in Windows
            strategy = '_'.join(os.path.basename(f).split('_')[:-2])
            if strategy == '':
                continue
            if strategy not in st_dfs:
                st_dfs[strategy] = []
            df = pd.read_csv(f)
            if df.shape[0] == 0:
                continue
            st_dfs[strategy].append(df)
    st_res = {}
    for strategy, dfs in st_dfs.items():
        df = pd.concat(dfs)
        df['profit_arith_sum'] = df['profit'].cumsum()
        invalid_profit = df[df['profit'] <= -1]
        if len(invalid_profit) > 0:
            print(f"Invalid profit found in {strategy}:")
            print(invalid_profit)
        df['profit_log'] = df['profit'].apply(lambda x: np.log(1+x) if x > -1 else np.log(0.001))
        df['profit_exp_sum'] = df['profit_log'].cumsum().apply(np.exp)
        df.to_csv(f"../output/{strategy}_trades.csv", index=False, float_format='%.6f')
        print(f"Concatenated trades saved to ../output/{strategy}_trades.csv")
        st_res[strategy] = df
    return st_res

def compress_trades(df: pd.DataFrame, strategy: str):
    # compress to one row per day
    df['dt'] = pd.to_datetime(df['dt'])
    df['date'] = df['dt'].dt.date
    daily_profit = df.groupby('date')['profit'].sum().reset_index()
    daily_profit['profit_arith_sum'] = daily_profit['profit'].cumsum()
    daily_profit['profit_log'] = daily_profit['profit'].apply(lambda x: np.log(1+x) if x > -1 else np.log(0.001))
    daily_profit['profit_exp_sum'] = daily_profit['profit_log'].cumsum().apply(np.exp)
    daily_profit['strategy'] = strategy
    daily_profit.to_csv(f"../output/{strategy}_trades_daily.csv", index=False, float_format='%.6f')
    print(f"Compressed trades saved to ../output/{strategy}_trades_daily.csv")
    return daily_profit

def main(bgdt: datetime.date, eddt: datetime.date,
        regress: bool, trade: bool, concat: bool):
    dts = []
    for dt in pd.date_range(bgdt, eddt, freq='W-MON'):
        dtstr = dt.strftime("%Y%m%d")
        dts.append(dtstr)
        print(f"Processing {dtstr}")
        try:
            if regress:
                s1.main(dt)
            if trade:
                s2.main(dt)
        except Exception as e:
            print(f"Error processing {dtstr}: {e}")
            # dump to error file
            with open(f"../output/{dtstr}_error.txt", "w") as f:
                f.write(f"Error processing {dtstr}: {e}\n")
                f.write(f"Traceback:\n")
                import traceback
                traceback.print_exc(file=f)
                f.write("\n")
            continue
        print(f"Finished {dtstr}")
    if concat:
        st_res = concat_trades(dts)
        for strategy, df in st_res.items():
            compress_trades(df, strategy)

@click.command()
@click.option('-b', '--bgdt', type=str, required=True, help="format is %Y%m%d")
@click.option('-e', '--eddt', type=str, required=True, help="format is %Y%m%d")
@click.option('--regress', type=bool, default=False, help="run regression")
@click.option('-t', '--trade', type=bool, default=False, help="simulate trades")
@click.option('-c', '--concat', type=bool, default=False, help="concatenate trades")
def click_main(bgdt: str, eddt: str, regress: bool, trade: bool, concat: bool):
    """
    主函数，解析命令行参数并调用其他函数。
    :param bgdt: 起始日期
    :param eddt: 结束日期
    """
    if (not regress) and (not trade) and (not concat):
        regress = True
        trade = True
        concat = True
        print("regress, trade, concat are all set to True")
    bgdt = datetime.datetime.strptime(bgdt, '%Y%m%d').date()
    eddt = datetime.datetime.strptime(eddt, '%Y%m%d').date()
    main(bgdt, eddt, regress, trade, concat)

if __name__ == '__main__':
    click_main()
