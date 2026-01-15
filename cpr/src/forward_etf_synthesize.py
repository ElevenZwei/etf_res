"""
这个脚本提供一个批处理过程，可以从期权的盘口数据，合成远期 ETF 的盘口数据。
1. 下载当日 ETF 现货盘口数据。
2. 计算 ETF 现货盘口当日的最高最低价格。
3. 下载行权价格距离 ETF 最高最低价格差异在 10% 以内的期权的当日盘口数据。
4. 对于 ETF 的每个 Tick，挑选这个 Tick 上下各自两档的期权编码。
5. 计算这个 Tick 上下两档期权各自合成的 ETF 远期价格，这里合成盘口上下各两档，还有中间价。
6. 根据期权各自合成的 ETF 远期盘口，组合成 ETF 远期盘口数据。中间价取各档中间价的加权平均价。
这个合成的盘口是有可能交叉的，因为合成的价格可能有微小的套利空间，通常不够手续费。

"""

from dl_oi import fetch_spot_data_new, fetch_option_md_new, get_nearest_expirydate
from typing import TypeAlias
import datetime
import polars as pl
import bisect
import math
from tqdm import tqdm
from typing import Optional
import click

from config import FORWARD_DIR


RISK_FREE_RATE = 0.016  # 无风险利率，年化

def dl_data(spot: str, dt: datetime.date):
    """
    下载现货和期权的盘口数据。
    """
    bg_dt, ed_dt = (datetime.datetime.combine(dt, datetime.time(0, 0)),
                    datetime.datetime.combine(dt, datetime.time(23, 59)))
    spot_df = pl.from_pandas(fetch_spot_data_new(spot, bg_dt, ed_dt))
    spot_df = spot_df.with_columns([
        pl.col('dt').cast(pl.Datetime).dt.convert_time_zone('Asia/Shanghai').alias('dt'),
        pl.col('spot_price').cast(pl.Float64),
        pl.col('spot').cast(pl.Utf8),
    ])
    spot_df = spot_df.sort('dt')
    spot_max = spot_df.select(pl.col('spot_price').max()).to_series()[0] or 0
    spot_min = spot_df.select(pl.col('spot_price').min()).to_series()[0] or 0
    print(f"Spot max: {spot_max}, min: {spot_min}")
    strike_max = spot_max * 1.1
    strike_min = spot_min * 0.9
    print(f"Strike range: {strike_min} - {strike_max}")
    expiry = get_nearest_expirydate(spot, dt)
    if expiry is None:
        raise ValueError(f"No expiry found for spot {spot} on date {dt}")
    all_opts_df = pl.from_pandas(fetch_option_md_new(
            spot, expiry, strike_min, strike_max, bg_dt, ed_dt))
    all_opts_df = all_opts_df.with_columns([
        pl.col('dt').cast(pl.Datetime).dt.convert_time_zone('Asia/Shanghai').alias('dt'),
        pl.col('strike').cast(pl.Float64),
        pl.col('callput').cast(pl.Int8),
        pl.col('bid_price').cast(pl.Float64),
        pl.col('ask_price').cast(pl.Float64),
        pl.col('oi').cast(pl.Float64),
    ])
    all_opts_df = all_opts_df.sort(['dt', 'strike', 'callput'])
    print(f"Option data rows: {all_opts_df.shape[0]}")

    return spot_df, all_opts_df


def get_strike_list(option_df: pl.DataFrame) -> list[float]:
    """
    从期权数据中提取行权价格列表，并排序。
    """
    strike_series = option_df.select(pl.col('strike')).to_series().unique().sort()
    strike_list = strike_series.to_list()
    return strike_list


def find_nearest_strikes(strike_list: list[float], price: float) -> (
        tuple[float, float, float, float]):
    """
    在行权价格列表中，找到最接近给定价格的上下两档行权价格。
    """
    if not strike_list:
        raise ValueError("Strike list is empty")

    def clamp(x):
        if x < 0:
            x = x + len(strike_list)
        return max(0, min(x, len(strike_list) - 1))

    pos = bisect.bisect_left(strike_list, price)
    # strike_list[pos - 1] < price <= strike_list[pos]
    if pos == 0:
        return (strike_list[0], strike_list[0],
                strike_list[clamp(1)], strike_list[clamp(2)])
    elif pos == len(strike_list):
        return (strike_list[clamp(-3)], strike_list[clamp(-2)],
                strike_list[-1], strike_list[-1])
    else:
        return (strike_list[clamp(pos - 2)], strike_list[clamp(pos - 1)],
                strike_list[clamp(pos)], strike_list[clamp(pos + 1)])


OptionSplitDict: TypeAlias = dict[float, tuple[pl.DataFrame, pl.DataFrame]]
def split_option_df_by_strike(all_opts_df: pl.DataFrame) -> OptionSplitDict:
    """
    按行权价格拆分期权数据。
    """
    strike_list = get_strike_list(all_opts_df)
    return {
            strike: (
                all_opts_df.filter((pl.col('strike') == strike) & (pl.col('callput') == 1)).sort('dt'),
                all_opts_df.filter((pl.col('strike') == strike) & (pl.col('callput') == -1)).sort('dt'),
            )
            for strike in strike_list
    }


def get_line_from_option_df(option_df: pl.DataFrame, dt: datetime.datetime) -> dict:
    """
    从期权数据中提取指定时间点的盘口数据行。
    """
    line_df = option_df.filter(pl.col('dt') <= dt).sort('dt', descending=True).head(1)
    if line_df.is_empty():
        raise ValueError(f"No data found for datetime {dt}")
    return line_df.select(pl.all()).row(0, named=True)


def mid_price_one_side(line: dict) -> Optional[float]:
    """
    计算单边盘口数据行的中间价。
    """
    bid_price = line['bid_price']
    ask_price = line['ask_price']
    if bid_price is None and ask_price is None:
        return None
    if bid_price is None:
        return ask_price
    if ask_price is None:
        return bid_price
    mid_price = (bid_price + ask_price) / 2.0
    return mid_price


def calc_mid_forward_price(call_line: dict, put_line: dict, strike_adjusted: float) -> Optional[float]:
    """
    计算看涨和看跌期权盘口数据行对应的远期价格中间价。
    """
    call_price = mid_price_one_side(call_line)
    put_price = mid_price_one_side(put_line)
    if call_price is None or put_price is None:
        return None
    forward_price = strike_adjusted + call_price - put_price
    return forward_price


def calc_forward_eat_price(call_prices: list[tuple[float, float]],
                   put_prices: list[tuple[float, float]],
                   strike_adjusted: float) -> (list[tuple[float, float]]):
    """
    从看涨和看跌期权价格列表中，计算远期价格的吃价。
    """
    res: list[tuple[float, float]] = []
    # remove from end
    call_prices.reverse()
    put_prices.reverse()
    while len(call_prices) > 0 and len(put_prices) > 0:
        forward_price = call_prices[-1][0] - put_prices[-1][0] + strike_adjusted
        forward_size = min(call_prices[-1][1], put_prices[-1][1])
        call_prices[-1] = (call_prices[-1][0], call_prices[-1][1] - forward_size)
        put_prices[-1] = (put_prices[-1][0], put_prices[-1][1] - forward_size)
        if call_prices[-1][1] == 0:
            call_prices.pop()
        if put_prices[-1][1] == 0:
            put_prices.pop()
        res.append((forward_price, forward_size))
    return res


def calc_ask_forward_price(call_line: dict, put_line: dict, strike_adjusted: float) -> (
        list[tuple[float, float]]):
    """
    计算看涨和看跌期权盘口数据行对应的远期价格卖价。
    """
    call_ask = call_line['ask_price']
    put_bid = put_line['bid_price']
    if call_ask is None or put_bid is None:
        return []

    call_prices = [(call_ask, call_line['ask_size'])]
    if call_line['ask2_price'] is not None and call_line['ask2_size'] is not None:
        call_prices.append((call_line['ask2_price'], call_line['ask2_size']))
    put_prices = [(put_bid, put_line['bid_size'])]
    if put_line['bid2_price'] is not None and put_line['bid2_size'] is not None:
        put_prices.append((put_line['bid2_price'], put_line['bid2_size']))
    return calc_forward_eat_price(call_prices, put_prices, strike_adjusted)


def calc_bid_forward_price(call_line: dict, put_line: dict, strike_adjusted: float) -> (
        list[tuple[float, float]]):
    """
    计算看涨和看跌期权盘口数据行对应的远期价格买价。
    """
    call_bid = call_line['bid_price']
    put_ask = put_line['ask_price']
    if call_bid is None or put_ask is None:
        return []
    call_prices = [(call_bid, call_line['bid_size'])]
    if call_line['bid2_price'] is not None and call_line['bid2_size'] is not None:
        call_prices.append((call_line['bid2_price'], call_line['bid2_size']))
    put_prices = [(put_ask, put_line['ask_size'])]
    if put_line['ask2_price'] is not None and put_line['ask2_size'] is not None:
        put_prices.append((put_line['ask2_price'], put_line['ask2_size']))
    return calc_forward_eat_price(call_prices, put_prices, strike_adjusted)


def calc_avg_eat_price(prices: list[tuple[float, float]], amount: int) -> Optional[float]:
    price_sum = 0.0
    size_sum = 0
    for price, size in prices:
        trade_size = min(size, amount - size_sum)
        price_sum += price * trade_size
        size_sum += trade_size
        if size_sum >= amount:
            break
    if size_sum == 0:
        return None
    avg_price = price_sum / size_sum
    return avg_price


def get_spot_price_at_dt(spot_df: pl.DataFrame, dt: datetime.datetime) -> float:
    spot_price = (spot_df.filter(pl.col('dt') <= dt)
                  .sort('dt', descending=True).head(1)
                  .select(pl.col('spot_price')).to_series()[0])
    return spot_price


def yield_call_put_lines(spot_df: pl.DataFrame, option_dict: OptionSplitDict, dt: datetime.datetime):
    """
    从现货数据和期权数据中，生成指定时间点的看涨和看跌期权盘口数据行对。
    这里我们选择跳过集合竞价时段的数据。
    挑选给定时间点之前最近的现货价格，然后找到最接近该价格的上下两档行权价格。
    """
    spot_price = get_spot_price_at_dt(spot_df, dt)
    if spot_price is None:
        return
    strike_tuple = find_nearest_strikes(
            list(option_dict.keys()), spot_price)

    for strike in strike_tuple:
        call_df, put_df = option_dict[strike]
        call_line = get_line_from_option_df(call_df, dt)
        put_line = get_line_from_option_df(put_df, dt)
        # 跳过集合竞价
        if call_line['ask_price'] == call_line['bid_price']:
            continue
        if put_line['ask_price'] == put_line['bid_price']:
            continue
        yield (call_line, put_line)


def calc_forward_prices_at_dt(spot_df: pl.DataFrame, option_dict: OptionSplitDict, dt: datetime.datetime) -> dict:
    """
    生成指定时间点的远期价格数据。
    """
    bid_prices: list[tuple[float, float]] = []
    ask_prices: list[tuple[float, float]] = []
    for call_line, put_line in yield_call_put_lines(spot_df, option_dict, dt):
        strike: float = call_line['strike']
        expiry: datetime.date = call_line['expiry']
        days_left = (expiry - dt.date()).days + 2
        strike_adjusted = strike * math.exp(-RISK_FREE_RATE * days_left / 365.0)
        bidps = calc_bid_forward_price(call_line, put_line, strike_adjusted)
        bid_prices.extend(bidps)
        askps = calc_ask_forward_price(call_line, put_line, strike_adjusted)
        ask_prices.extend(askps)
    expiry = option_dict[next(iter(option_dict))][0].select(pl.col('expiry')).to_series()[0]
    spot_name = spot_df.select(pl.col('spot')).to_series()[0]
    forward_name = spot_name + '_' + (expiry.strftime('%Y%m') if expiry else 'unknown')
    spot_price = get_spot_price_at_dt(spot_df, dt)
    ask_prices.sort(key=lambda x: x[0])
    bid_prices.sort(key=lambda x: x[0], reverse=True)
    ask_price = ask_prices[0][0] if ask_prices else None
    bid_price = bid_prices[0][0] if bid_prices else None
    mid_price = (ask_price + bid_price) / 2.0 if ask_price is not None and bid_price is not None else None
    # 计算总的吃价数量
    ask_size_sum = sum(size for _, size in ask_prices)
    bid_size_sum = sum(size for _, size in bid_prices)
    # 计算平均吃价价格
    ask_avg_30_price = calc_avg_eat_price(ask_prices, 30)
    bid_avg_30_price = calc_avg_eat_price(bid_prices, 30)
    ask_avg_100_price = calc_avg_eat_price(ask_prices, 100)
    bid_avg_100_price = calc_avg_eat_price(bid_prices, 100)
    # 组合结果
    return {
            'dt': dt,
            'name': forward_name,
            'spot_price': spot_price,
            'mid_price': mid_price,

            'ask_price': ask_prices[0][0] if ask_prices else None,
            'ask_size': ask_prices[0][1] if ask_prices else None,
            'bid_price': bid_prices[0][0] if bid_prices else None,
            'bid_size': bid_prices[0][1] if bid_prices else None,

            'ask2_price': ask_avg_30_price,
            'ask2_size': 30,
            'bid2_price': bid_avg_30_price,
            'bid2_size': 30,
            'ask3_price': ask_avg_100_price,
            'ask3_size': 100,
            'bid3_price': bid_avg_100_price,
            'bid3_size': 100,

            'ask_size_sum': ask_size_sum,
            'bid_size_sum': bid_size_sum,
    }


def make_forward_price_series(spot_df: pl.DataFrame, option_dict: OptionSplitDict) -> pl.DataFrame:
    """
    生成远期价格时间序列数据。
    """
    dt_list = spot_df.select(pl.col('dt')).to_series().to_list()
    records = []
    for dt in tqdm(dt_list, desc="Calculating forward prices"):
        record = calc_forward_prices_at_dt(spot_df, option_dict, dt)
        records.append(record)
    forward_df = pl.DataFrame(records)
    return forward_df


def synthesize_main(spot: str, dt: datetime.date):
    spot_df, all_opts_df = dl_data(spot, dt)
    opt_dict = split_option_df_by_strike(all_opts_df)
    forward_df = make_forward_price_series(spot_df, opt_dict)
    forward_df.write_csv(FORWARD_DIR / f'forward_price_{spot}_{dt.strftime("%Y%m%d")}.csv')
    return forward_df


@click.command()
@click.option('-s', '--spot', type=click.Choice(['159915', '510500']), required=True, help='Spot code to synthesize')
@click.option('-d', '--date', type=click.DateTime(formats=["%Y-%m-%d"]), required=True, help='Date to synthesize (YYYY-MM-DD)')
def click_main(spot: str, date: datetime.datetime):
    synthesize_main(spot, date.date())


if __name__ == '__main__':
    click_main()


