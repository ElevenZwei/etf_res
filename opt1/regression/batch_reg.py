"""
对于一段时间，用每周一作为起始点训练之前的数据，然后预测未来一周的值，进行交易。
"""

import click
import os
import datetime
import pandas as pd

import linear_reg_1 as s1
import reg_trade_1 as s2

def concat_trades(dts: list[str]):
    """
    将所有交易结果合并到一个文件中。
    :param dts: 日期列表
    """
    dfs = []
    for dt in dts:
        fpath = f"../output/{dt}/reg1_trades_validate.csv"
        if os.path.exists(fpath):
            df1 = pd.read_csv(fpath)
            dfs.append(df1)
        else:
            print(f"File {fpath} does not exist.")
    df = pd.concat(dfs)
    df['profit_accu'] = df['profit'].cumsum()
    df.to_csv("../output/reg1_trades.csv", index=False, float_format='%.6f')
    print(f"Concatenated trades saved to ../output/reg1_trades.csv")

def main(bgdt: datetime.date, eddt: datetime.date, concat_only: bool = False):
    dts = []
    for dt in pd.date_range(bgdt, eddt, freq='W-MON'):
        dtstr = dt.strftime("%Y%m%d")
        dts.append(dtstr)
        if not concat_only:
            print(f"Processing {dtstr}")
            try:
                s1.main(dt)
                s2.main(dt)
            except Exception as e:
                print(f"Error processing {dtstr}: {e}")
                continue
            print(f"Finished {dtstr}")
    concat_trades(dts)

@click.command()
@click.option('-b', '--bgdt', type=str, required=True, help="format is %Y%m%d")
@click.option('-e', '--eddt', type=str, required=True, help="format is %Y%m%d")
@click.option('--concat-only', is_flag=True, help="Only concatenate trades")
def click_main(bgdt: str, eddt: str, concat_only: bool):
    """
    主函数，解析命令行参数并调用其他函数。
    :param bgdt: 起始日期
    :param eddt: 结束日期
    """
    bgdt = datetime.datetime.strptime(bgdt, '%Y%m%d').date()
    eddt = datetime.datetime.strptime(eddt, '%Y%m%d').date()
    main(bgdt, eddt, concat_only)

if __name__ == '__main__':
    click_main()
