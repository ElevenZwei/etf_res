"""
这个脚本用于线性回归模型的训练和评估。
从 sample_mid.csv 中得到一个线性回归模型，使用 statsmodel.api.OLS 进行训练。
"""

import click
import os
import datetime
import pandas as pd
import statsmodels.api as sm
import numpy as np

def read_csv(fpath: str):
    """
    读取 CSV 文件，并返回数据框。
    :param fpath: CSV 文件路径
    :return: 数据框
    """
    df = pd.read_csv(fpath)
    df['dt'] = pd.to_datetime(df['date'])
    df.drop(columns=['date'], inplace=True)
    df.rename(columns={
        'option-ret': 'opt_ret',
        'option-ret-var': 'opt_ret_var',
        'if-ret': 'index_ret',
        'if-ret-var': 'index_ret_var',
        'option-price(shi1)': 'opt_price',
    }, inplace=True)
    df = df.sort_values(by='dt')
    return df


def clip_df(df: pd.DataFrame, split_dt: datetime.date,
            train_days: int, validate_days: int):
    """
    对数据框进行裁剪，返回训练集和验证集。
    :param df: 数据框
    :param bg_dt: 起始日期
    :param train_days: 训练集天数
    :param validate_days: 验证集天数
    :return: 训练集和验证集
    """
    split_dt = pd.to_datetime(split_dt)
    bg_dt = split_dt - datetime.timedelta(days=train_days)
    ed_dt = split_dt + datetime.timedelta(days=validate_days)
    df = df[(df['dt'] >= bg_dt) & (df['dt'] < ed_dt)]
    train_df = df[df['dt'] < split_dt]
    validate_df = df[df['dt'] >= split_dt]
    return train_df, validate_df


def train_model(train_df: pd.DataFrame):
    """
    训练线性回归模型。
    :param train_df: 训练集数据框
    :return: 训练好的模型
    """
    X = train_df[['index_ret', 'index_ret_var', 'opt_ret_var']]
    # X = train_df[['index_ret', 'index_ret_var']]
    y = train_df['opt_ret']
    X = sm.add_constant(X)  # 添加常数项
    model = sm.OLS(y, X).fit()
    print(model.summary())
    return model


def evaluate_model(model, validate_df: pd.DataFrame):
    """
    评估模型性能。
    :param model: 训练好的模型
    :param validate_df: 验证集数据框
    :return: 模型评估结果
    """
    X = validate_df[['index_ret', 'index_ret_var', 'opt_ret_var']]
    # X = validate_df[['index_ret', 'index_ret_var']]
    y = validate_df['opt_ret']
    X = sm.add_constant(X)  # 添加常数项
    y_pred = model.predict(X)
    
    # 计算均方误差和决定系数
    # mse = np.mean((y - y_pred) ** 2)
    # r_squared = model.rsquared
    # print(f'MSE: {mse}, R-squared: {r_squared}')
    # return mse, r_squared
    
    return pd.DataFrame({
        'dt': validate_df['dt'],
        'real': y,
        'pred': y_pred,
        'residual': y - y_pred,
    })


def residual_stat(df: pd.DataFrame, label: str, col: str = 'residual'):
    resi = df[col].values
    mean = np.mean(resi)
    stdev = np.std(resi)
    skew = np.mean((resi - mean) ** 3) / (stdev ** 3)
    kurt = np.mean((resi - mean) ** 4) / (stdev ** 4) - 3
    median = np.median(resi)
    percentile_80 = np.percentile(resi, 80)
    percentile_20 = np.percentile(resi, 20)
    print(f"Label: {label}, Mean: {mean}, Std: {stdev}, Median: {median}, "
          f"Skew: {skew}, Kurtosis: {kurt}, 80th Percentile: {percentile_80}, "
          f"20th Percentile: {percentile_20}")
    return {
        'label': label,
        'mean': mean, 'std': stdev,
        'skew': skew, 'kurtosis': kurt,
        'median': median,
        'p80': percentile_80,
        'p20': percentile_20,
    }


INPUT_DIR = '../input'
OUTPUT_DIR = '../output'

def main(split_date: datetime.date):
    df = read_csv(f'{INPUT_DIR}/sample_mid.csv')
    train_df, validate_df = clip_df(df, split_date, 30, 7)
    model = train_model(train_df)

    output_date_dir = f'{OUTPUT_DIR}/{split_date.strftime("%Y%m%d")}'
    os.makedirs(output_date_dir, exist_ok=True)
    train_res = evaluate_model(model, train_df)
    train_res.to_csv(f'{output_date_dir}/pred_train.csv', index=False)
    validate_res = evaluate_model(model, validate_df)
    validate_res.to_csv(f'{output_date_dir}/pred_validate.csv', index=False)

    train_resi_stat = residual_stat(train_res, 'train')
    validate_resi_stat = residual_stat(validate_res, 'validate')
    resi_stat = pd.DataFrame([train_resi_stat, validate_resi_stat])
    resi_stat.to_csv(f'{output_date_dir}/resi_stat.csv', index=False)


@click.command()
@click.option('-d', '--split-date', type=str, required=True,
              help="format is %Y%m%d")
def click_main(split_date: str):
    """
    主函数，解析命令行参数并调用其他函数。
    :param split_date: 分割日期
    """
    split_date = datetime.datetime.strptime(split_date, '%Y%m%d').date()
    main(split_date)


if __name__ == '__main__':
    click_main()
