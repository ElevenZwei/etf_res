"""Analyze forward ETF price data for anomalies and statistics."""

import polars as pl
import datetime

from config import FORWARD_DIR

forward_df = pl.read_csv(FORWARD_DIR / 'forward_price_159915_20260112.csv')
forward_df = forward_df.with_columns([
    pl.col('dt').cast(pl.Datetime),
])
forward_df = forward_df.sort('dt')
forward_df = forward_df.filter(
        pl.col('dt').dt.time()
        .is_between(datetime.time(9, 30, 1), datetime.time(14, 56, 59))
)
forward_df = forward_df.with_columns([
    (pl.col('ask_price') - pl.col('bid_price')).alias('spread'),
    (pl.col('mid_price') - pl.col('spot_price')).alias('mid_spot_diff'),
])
print(f"Total lines in forward_df: {forward_df.height}")
# print lines with null mid_price
forward_df_null = forward_df.filter(pl.col('mid_price').is_null())
if forward_df_null.height > 0:
    print("Lines with null mid_price:")
    print(forward_df_null)
# print lines where mid_price is out of [ask_price, bid_price]
forward_df_deviate = forward_df.filter(
        pl.col('mid_price').is_not_null() &
        (
            (pl.col('bid_price') < pl.col('ask_price')) &
            ((pl.col('mid_price') < pl.col('bid_price')) |
            (pl.col('mid_price') > pl.col('ask_price')))
        )
)
if forward_df_deviate.height > 0:
    print("Lines where mid_price is out of [bid_price, ask_price]:")
    print(forward_df_deviate.select(['dt', 'mid_price', 'bid_price', 'ask_price']))
forward_df_negative_spread = forward_df.filter(pl.col('spread') < 0)
if forward_df_negative_spread.height > 0:
    print("Lines with negative spread:")
    print(forward_df_negative_spread.select(['dt', 'bid_price', 'ask_price', 'spread']))
# print statistics of spread
forward_df_spread_stats = forward_df.select(['spread']).describe()
print("Spread statistics:")
print(forward_df_spread_stats)
# print statistics of mid_spot_diff
forward_df_mid_spot_diff_stats = forward_df.select(['mid_spot_diff']).describe()
print("Mid-Spot Difference statistics:")
print(forward_df_mid_spot_diff_stats)

forward_df_select = forward_df.select(['dt', 'mid_spot_diff', 'spread', 'mid_price', 'spot_price'])
# calculate avg by minute
forward_df_select = forward_df_select.group_by(
            pl.col('dt').dt.truncate('1m')
        ).agg([
            pl.col('mid_spot_diff').mean().alias('avg_mid_spot_diff'),
            pl.col('spread').mean().alias('avg_spread'),
            pl.col('mid_price').mean().alias('avg_mid_price'),
            pl.col('spot_price').mean().alias('avg_spot_price'),
        ]).rename({'dt': 'dt'})
forward_df_select.write_csv(FORWARD_DIR / 'forward_price_159915_20260112_analyze.csv')



# forward_df_dep = pl.read_csv(FORWARD_DIR / 'forward_price_159915_20260112_dep.csv')
# forward_df_dep = forward_df_dep.with_columns([
#     pl.col('dt').cast(pl.Datetime),
# ])
# cols = ['dt', 'ask_price', 'ask_size', 'bid_price', 'bid_size']
# forward_df_select = forward_df.select(cols)
# forward_df_dep_select = forward_df_dep.select(cols)
# # compare the two dataframes
# diff_df = (forward_df_select.join(forward_df_dep_select, on='dt', how='inner', suffix='_2')
#         .filter(
#                 ((pl.col('ask_price') - pl.col('ask_price_2')).abs() > 1e-6) |
#                 (pl.col('ask_size') != pl.col('ask_size_2')) |
#                 ((pl.col('bid_price') - pl.col('bid_price_2')).abs() > 1e-6) |
#                 (pl.col('bid_size') != pl.col('bid_size_2'))
#         ))
# if diff_df.height > 0:
#     print("Differences between the two forward price files:")
#     print(diff_df)
#
