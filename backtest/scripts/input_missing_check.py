# check missing time spans in input data
# 

import pandas as pd
import click


def check_missing_time_spans(df, time_col, max_interval_seconds=120):
    df = df.sort_values(by=time_col)
    df[time_col] = pd.to_datetime(df[time_col])
    df = df.set_index(time_col)
    df = df.tz_convert('Asia/Shanghai')
    df = df.reset_index()
    df['time_diff'] = df[time_col].diff().dt.total_seconds()
    mi = df[df['time_diff'] > max_interval_seconds].copy()
    mi['start_time'] = df[time_col].shift(1)
    mi['end_time'] = mi[time_col]
    mi = mi[~(
        ((mi['start_time'].dt.time >= pd.to_datetime('11:29:40').time())
         & (mi['end_time'].dt.time <= pd.to_datetime('13:00:20').time()))
        |
        ((mi['start_time'].dt.time >= pd.to_datetime('14:59:40').time())
         & (mi['end_time'].dt.time <= pd.to_datetime('09:30:20').time()))
        | (mi['start_time'].dt.weekday > 4)
        | (mi['end_time'].dt.weekday > 4)
        )]

    for idx, row in mi.iterrows():
        start_time = df.loc[idx - 1, time_col]
        end_time = row[time_col]
        print(f"Missing interval from {start_time} to {end_time}, gap: {row['time_diff']} seconds")
    if mi.empty:
        print("No missing intervals found.")
    return mi




def main(fpath: str):
    df = pd.read_csv(fpath)
    missing = check_missing_time_spans(df, 'dt', max_interval_seconds=70)
    print(missing)

    missing_starts = missing['start_time']
    effective_missing_starts = missing_starts[
            missing_starts.dt.time <= pd.to_datetime('14:59:40').time()]
    print("Effective missing starts (before market close):\n", effective_missing_starts)

    missing_ends = missing['end_time']
    effective_missing_ends = missing_ends[
            missing_ends.dt.time >= pd.to_datetime('09:30:20').time()]
    print("Effective missing ends (after market open):\n", effective_missing_ends)

    missing_dates = pd.concat([
        effective_missing_starts, effective_missing_ends]).dt.date.unique()
    missing_dates = sorted(missing_dates)
    print("Missing dates:", missing_dates)


@click.command()
@click.option('-f', '--file', type=click.Path(exists=True), required=True, help='Path to the CSV file to check')
def click_main(file: str):
    main(file)

if __name__ == '__main__':
    click_main()
    # For testing purposes
    # main('../data/input/spot_159915_2025_dsp.csv')
