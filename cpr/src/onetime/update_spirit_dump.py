import polars as pl

# fix spirit table from old table format to new table format
def fix_spirit_table(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        pl.col('username').replace_strict({
            '00535726': '',
            '505000011810': 's1_159915',
            '505000011915': 's2_159915',
            '88103003129': 's3_159915',
            '208828': 'future_trade_test',
        }).alias('config_name'),
    ])
    return df


def fix_spirit_persistence(df: pl.DataFrame) -> pl.DataFrame:
    df = fix_spirit_table(df)
    df = df.select([
        'id', 'username', 'config_name',
        'spirit', 'key', 'value', 'updated_at',
    ])
    return df


def fix_spirit_position(df: pl.DataFrame) -> pl.DataFrame:
    df = fix_spirit_table(df)
    df = df.select([
        'id', 'username', 'config_name',
        'spirit', 'code', 'position', 'updated_at',
    ])
    return df


def main():
    df1 = pl.read_csv('../../data/dump/spirit_persistence.csv')
    df1 = fix_spirit_persistence(df1)
    df1.write_csv('../../data/dump/spirit_persistence_fixed.csv')

    df2 = pl.read_csv('../../data/dump/spirit_persistence_history.csv')
    df2 = fix_spirit_persistence(df2)
    df2.write_csv('../../data/dump/spirit_persistence_history_fixed.csv')

    df3 = pl.read_csv('../../data/dump/spirit_position.csv')
    df3 = fix_spirit_position(df3)
    df3.write_csv('../../data/dump/spirit_position_fixed.csv')

    df4 = pl.read_csv('../../data/dump/spirit_position_history.csv',
                    schema={
                        'id': pl.Int64,
                        'username': pl.Utf8,
                        'spirit': pl.Utf8,
                        'code': pl.Utf8,
                        'position': pl.Utf8,
                        'updated_at': pl.Utf8
                    })
    df4 = fix_spirit_position(df4)
    df4.write_csv('../../data/dump/spirit_position_history_fixed.csv')


if __name__ == '__main__':
    main()




