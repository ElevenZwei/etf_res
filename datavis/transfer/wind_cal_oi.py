import pandas as pd
import datetime
import os
import click
import glob

def cal_day(spot: str, dt: datetime.date):
    fs = get_files(spot, dt)
    if len(fs) == 0:
        print(f"no files for {spot} on {dt}")
        return
    dfs = [pd.read_csv(f) for f in fs]

    # 有些数据被裁剪到空了是一个问题，我甚至没法筛选它们的到期日。
    # 不过我可以用一个命令行来找出这些文件。
    # find /your/folder/path -type f -exec awk 'NR>1{exit} END{if (NR==1) print FILENAME}' {} +
    empty_fs = [fs[x] for x, df in enumerate(dfs) if df.shape[0] == 0]
    if len(empty_fs) > 0:
        print(f"empty dfs: {empty_fs}")

    # 只要有数据就可以了，第一行数据就可以了。
    non_empty_dfs = [df for df in dfs if df.shape[0] > 0]
    # get the first line of every file
    lines = [df.iloc[0] for df in non_empty_dfs]
    if len(lines) == 0:
        print(f"no data for {spot} on {dt}")
        return
    df_exp = pd.DataFrame(lines)
    min_exp = df_exp['expirydate'].min()
    codes = df_exp[df_exp['expirydate'] == min_exp]['code'].to_list()
    # print(f"min expiry date: {min_exp}, codes: {codes}")

    # use min_exp to filter dfs list
    min_exp_dfs = [df for df in non_empty_dfs if df['expirydate'].iloc[0] == min_exp]
    big_df = pd.concat(min_exp_dfs, ignore_index=True)
    big_df = big_df[['dt', 'expirydate', 'callput', 'strike', 'code', 'openinterest']]
    dtstr = dt.strftime('%Y%m%d')
    # big_df.to_csv(f'{spot}_{dtstr}.csv', index=False)

    call_df = big_df[big_df['callput'] == 1]
    put_df = big_df[big_df['callput'] == -1]
    call_sum = pivot_oi_sum(call_df)
    call_sum = call_sum.rename(columns={'oi_sum': 'call_oi_sum'})
    put_sum = pivot_oi_sum(put_df)
    put_sum = put_sum.rename(columns={'oi_sum': 'put_oi_sum'})
    cp_sum = pd.merge(call_sum, put_sum, left_index=True, right_index=True, how='outer')
    cp_sum['spot'] = spot
    cp_sum['pc'] = cp_sum['put_oi_sum'] - cp_sum['call_oi_sum']
    return cp_sum

def pivot_oi_sum(df: pd.DataFrame):
    # dup = df[df.duplicated(subset=['dt', 'strike'], keep=False)]
    # dup.to_csv('dup.csv', index=True)
    df = df.pivot(index='dt',
            columns='code', values='openinterest')
    df = df.ffill().bfill()
    df['oi_sum'] = df.sum(axis=1)
    df = df[['oi_sum']]
    return df

def get_files(spot: str, dt: datetime.date):
    dt_str = dt.strftime('%Y%m%d')
    path = f'../db/bar/{spot}/{dt_str}/bar*.csv'
    files = glob.glob(path)
    if len(files) == 0:
        print(f"no files for {spot} on {dt}")
        return []
    return files
    
def main(spot: str, bg_str: str, ed_str: str):
    bg_dt = datetime.datetime.strptime(bg_str, '%Y%m%d').date()
    ed_dt = datetime.datetime.strptime(ed_str, '%Y%m%d').date()
    for dt in pd.date_range(bg_dt, ed_dt):
        if dt.weekday() >= 5:
            continue
        if not os.path.exists(f'../db/bar/{spot}/{dt.strftime("%Y%m%d")}/'):
            print(f"no data for {spot} on {dt}")
            continue
        cp_sum = cal_day(spot, dt)
        cp_sum = cp_sum.to_csv(f'../db/oi/oi_sum_{spot}_{dt.strftime("%Y%m%d")}.csv')

if __name__ == '__main__':
    main('510300.SH', '20250103', '20250402')