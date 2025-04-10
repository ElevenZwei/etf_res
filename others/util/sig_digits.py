"""
读取一个 csv 文件，
对它的每个字段，保留到 6 位有效数字。
然后储存起来。
"""

import pandas as pd
import numpy as np
import click

def save_df(df: pd.DataFrame, fpath: str):
    """
    保存数据框到 CSV 文件。
    :param df: 数据框
    :param fpath: CSV 文件路径
    """
    # Apply formatting to limit float fields to 6 significant digits
    for col in df.select_dtypes(include=['float']).columns:
        df[col] = df[col].apply(lambda x: round(x, 6 - int(np.floor(np.log10(abs(x)))) - 1) if x != 0 else 0)

    # Save the dataframe to a CSV file
    df.to_csv(fpath, index=False)

def main(input: str, output: str):
    """
    主函数，读取 CSV 文件，处理数据，保存结果。
    :param input: 输入 CSV 文件路径
    :param output: 输出 CSV 文件路径
    """
    df = pd.read_csv(input)
    save_df(df, output)
    print(f"Processed {input} and saved to {output}")


@click.command()
@click.option('-i', '--input', type=str, required=True, help="Input CSV file path")
@click.option('-o', '--output', type=str, required=True, help="Output CSV file path")
def click_main(input: str, output: str):
    """
    命令行接口，解析输入输出参数并调用主函数。
    :param input: 输入 CSV 文件路径
    :param output: 输出 CSV 文件路径
    """
    main(input, output)


if __name__ == '__main__':
    click_main()
