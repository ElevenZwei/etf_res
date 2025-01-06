# 这个文件读取 dsp_conv 里面的数据文件，并且计算 spot 和 oi 曲面的交线。
# 输出只需要一个文件，这个文件里面要涵盖这个时间 spot 和各种曲面的相交结果。

# 对于每一个文件，需要计算这个时间的 spot 和 oi_c_gau_2d, oi_p_gau_2d, oi_cp_gau_2d 三者的交线位置

import click
import pandas as pd
import pathlib
import glob

from dsp_config import DATA_DIR, gen_wide_suffix

def intersect_lines(spot_df: pd.DataFrame, oi_df: pd.DataFrame):
    spot_df['dt'] = pd.to_datetime(spot_df['dt'])
    spot_df['spot_price'] = pd.to_numeric(spot_df['spot_price'])
    spot_df = spot_df.rename(columns={'spot_price': 'price'}).sort_values(['price', 'dt'])
    spot_df = spot_df[['dt', 'price']]

    oi_df['dt'] = pd.to_datetime(oi_df['dt'])
    oi_df['strike'] = pd.to_numeric(oi_df['strike'])
    oi_df = oi_df.rename(columns={'strike': 'price'}).sort_values(['price', 'dt'])

    merged_df = pd.merge_asof(spot_df, oi_df, on='price', by='dt', direction='nearest')
    return merged_df

def intersect_merge_files(spot: str, suffix: str, wide: bool):
    wide_suffix = gen_wide_suffix(wide)
    suffix = f'{suffix}{wide_suffix}'
    spot_df = pd.read_csv(f'{DATA_DIR}/dsp_conv/spot_{spot}_{suffix}.csv')
    oi_fs = glob.glob(f'{DATA_DIR}/dsp_conv/strike_oi_smooth_{spot}_{suffix}_*.csv')
    inter_dfs = []
    for oi_fp in oi_fs:
        csv_name = pathlib.Path(oi_fp).stem
        csv_parts = csv_name.split('_')
        ts_sigma = csv_parts[-2]
        strike_sigma = csv_parts[-1]
        oi_df = pd.read_csv(oi_fp)
        in_df = intersect_lines(spot_df, oi_df)
        in_df = in_df.set_index(['dt'])
        in_df = in_df[['oi_c_gau_2d', 'oi_p_gau_2d', 'oi_cp_gau_2d']]
        in_df = in_df.rename(columns={
            'oi_c_gau_2d': f'oi_c_{ts_sigma}_{strike_sigma}',
            'oi_p_gau_2d': f'oi_p_{ts_sigma}_{strike_sigma}',
            'oi_cp_gau_2d': f'oi_cp_{ts_sigma}_{strike_sigma}',
        })
        print(in_df.columns)
        inter_dfs.append(in_df)
    spot_df = spot_df.set_index('dt')
    merged = pd.concat([spot_df, *inter_dfs], axis=1)
    merged.to_csv(f'{DATA_DIR}/dsp_conv/merged_{spot}_{suffix}.csv')

@click.command()
@click.option('-s', '--spot', type=str, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, help="csv file name suffix.")
@click.option('--wide', type=bool, default=False, help="wide plot.")
def click_main(spot: str, suffix: str, wide: bool):
    intersect_merge_files(spot, suffix, wide=wide)

if __name__ == '__main__':
    click_main()
    # intersect_files('159915', '20241017m')
    # intersect_files('159915', '20241022')
    # intersect_files('159915', '20241101')
    # intersect_files('159915', '20241104')
    # intersect_files('159915', '20241105')
    # intersect_files('159915', '20241106')
    # intersect_files('159915', '20241108')
    # intersect_files('159915', '20241112')
    # intersect_files('510050', '20241114_am')
    pass
