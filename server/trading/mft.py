from functools import partial
from math import nan

from pandas import DataFrame, merge, concat, to_datetime

from data.http.exante import ExanteClient
from app.asset import Future
from utils import swallow_and_log_exception
from app.cache import cache_result

status = 'live'
exante_client = None


def get_exante_client():
    global exante_client
    if exante_client is None:
        exante_client = ExanteClient.create(status=status)
    return exante_client


def estimate_cost(info, price):
    fx_costs = 2 / 10000
    futures_costs = info['mpi'] / price['close']
    fund_costs = 0
    interest = (75 / 100) * (15 / 250) * (2.1 / 100)
    return fx_costs + futures_costs + fund_costs + interest


def start_and_end_timestamps(df):
    return df.iloc[-1]["timestamp"], df.iloc[0]["timestamp"]


@swallow_and_log_exception(default=float('nan'))
@cache_result(3*60*60, validate=lambda x: isinstance(x, float))
def fair_spread(index_symbol, future_symbol):
    exante_client = get_exante_client()
    index_bars = exante_client.OHLC(index_symbol, bar_length_seconds=300,
                                    number_of_bars=20)  # OHLC("SPX.INDEX", 60, 2)
    start_time, end_time = start_and_end_timestamps(index_bars)
    future_bars = exante_client.OHLC(future_symbol, bar_length_seconds=300, number_of_bars=20,
                                     start_time=start_time, end_time=end_time)
    df = merge(future_bars, index_bars, on="datetime")
    df = df.rename(columns={"close_x": "future_close", "close_y": "index_close"})
    df.loc[:, "spread"] = df["future_close"] - df["index_close"]
    df = df.sort_values(by="spread")
    quartile = int(df["spread"].count() / 4)
    df = df.iloc[quartile:-quartile]
    spread = df["spread"].mean()
    return spread


signal_cols = ['index', 'future', 'future price', 'index price', 'fair spread', 'time', 'gross_signal', 'net_signal']


def signal_error():
    vals = ['n/a', 'n/a', nan, nan, nan, nan, nan, nan]
    return DataFrame([vals], columns=signal_cols)


@cache_result(24*60*60)
def get_nearest(index):
    exante_client = get_exante_client()
    return exante_client.nearest(index)


@cache_result(60*60)
def get_latest_index(symbol):
    exante_client = get_exante_client()
    return exante_client.latest_price(symbol)


@swallow_and_log_exception(default=signal_error())
def calculate_signal(index: str):
    exante_client = get_exante_client()
    index_symbol = index + exante_client.INDEX_SUFFIX
    info = get_nearest(index)
    future_symbol = info["id"]
    spread = fair_spread(index_symbol, future_symbol)
    future_price = exante_client.latest_price(future_symbol)
    index_price = get_latest_index(index_symbol)
    start_price = index_price["close"] + spread
    gross_signal = (future_price["close"] - start_price) / start_price
    cost = estimate_cost(info, future_price)
    if gross_signal > 0:
        net_signal = max(gross_signal - cost, 0)
    else:
        net_signal = min(gross_signal + cost, 0)
    data = [{"Index": index_symbol,
             "Future": future_symbol,
             "Future Price": future_price["close"],
             "Index Price": index_price["close"],
             "Fair Spread": spread,
             "Time": future_price["datetime"],
             "Gross Signal": gross_signal,
             'Net Signal': net_signal}]
    return DataFrame(data)


def calculate_signals(*indexes):
    signals = []
    for index in indexes:
        signal = calculate_signal(index)
        signals.append(signal)
    return concat(signals)


def contract_information():
    exante_client = get_exante_client()
    symbols = ['SPX', 'TOPIX', 'HSI', 'RTY']
    infos = exante_client.batch_api_request(exante_client.nearest, symbols)
    currencies, ids = set(), list()
    for info in infos:
        ids.append(info['id'])
        currencies.add(info['currency'])
    prices = exante_client.batch_api_request(exante_client.latest_price, ids)
    crossrates = exante_client.batch_api_request(exante_client.gbp_rate, currencies)
    df = DataFrame(infos)
    prices = DataFrame(prices)
    df = df.merge(prices, left_on='id', right_on='symbol', how='left')
    df = df.merge(DataFrame(crossrates), left_on='currency', right_on='currency_from', how='left')
    df = df.merge(Future.CONTRACT_MULTS, left_on='group', right_on='exante_group', how='left')
    df.loc[:, 'expiration'] = to_datetime(df['expiration'], unit="ms")
    df.loc[:, "Contract Size (£)"] = df['close'] * df['rate'] * df['mult']
    column_map = {"name": "Name", "country": "Country", "exchange": "Exchange", "id": "Exante ID",
                  "currency": "Currency", "expiration": "Expiration", "datetime": "As Of"}
    df = df.rename(columns=column_map)
    return df


def signal_data():
    return calculate_signals("SPX", "TOPIX", "HSI")


if __name__ == "__main__":
    #pass
    exante_client = ExanteClient.create(status='live')
    index = 'TOPIX'

    #data = exante_client.api_request("groups")
    info = get_nearest(index)
    index_symbol = index + exante_client.INDEX_SUFFIX
    info = exante_client.nearest(index)
    future_symbol = info["id"]
    #spread = fair_spread(index_symbol, future_symbol)

    index_bars = exante_client.OHLC(index_symbol, bar_length_seconds=300,
                                    number_of_bars=20)
    #data = exante_client.OHLC(index_symbol, bar_length_seconds=300, number_of_bars=20)
    data = calculate_signal(index)
    print(data)
