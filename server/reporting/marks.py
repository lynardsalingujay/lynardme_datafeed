from datetime import datetime

from app.asset import Asset
from app.enums import buy, sell, fund, fx_forward, future, fx_spot, reyl, cash, interest, exante, bloomberg
from app.models import Price, to_dataframe
from data.http.exante import ExanteClient
from pandas import concat, DataFrame


def symbols(df, asset_types=None):
    if asset_types is None:
        _df = df
    else:
        _df = df[df['asset_type'].isin(asset_types)]
    syms = list(set(_df['symbol'].values))
    return syms


def pretend_forwards_are_spots(asset_types):
    if asset_types is None:
        return None
    _asset_types = []
    for asset_type in asset_types:
        if asset_type == fx_forward:
            _asset_types.append(fx_spot)
        else:
            _asset_types.append(asset_type)
    return _asset_types


def get_db_prices(positions, as_of=None, time=None, asset_types=None, source=None):
    syms = symbols(positions, asset_types)
    _asset_types = pretend_forwards_are_spots(asset_types)
    select_map = dict(symbol__in=syms)
    if as_of is not None:
        select_map['as_of'] = as_of
    if time is not None:
        select_map['time'] = time
    if source is not None:
        select_map['source'] = source
    if _asset_types is not None:
        select_map['asset_type__in'] = _asset_types
    prices = Price.objects.filter(**select_map)
    prices = to_dataframe(prices)
    return prices


def get_gbp_exchange_rates(positions, as_of=None, source=None):
    currencies = list(set(positions['currency'].values))
    cash_fxs = dict()
    for cur in currencies:
        if cur != 'GBP':
            cash_fxs['GBP' + cur + ' Curncy'] = cur
    prices = Price.objects.filter(source=source, symbol__in=cash_fxs.keys(), asset_type=fx_spot, as_of=as_of, time=as_of)
    prices = to_dataframe(prices)
    prices.loc[:, 'symbol'] = prices['symbol'].replace(cash_fxs)
    prices.loc[:, 'price'] = 1 / prices['price']
    name_map = {'symbol': 'currency', 'price': 'fx_rate'}
    prices = prices.rename(columns=name_map)[name_map.values()]
    prices = concat([prices, DataFrame([['GBP', 1]], columns=['currency', 'fx_rate'])])
    return prices


def exante_futures_marks(as_of, df):
    exante_client = ExanteClient.create(status='live')
    bbg_symbols = symbols(df, [future])
    bars = []
    for bbg_symbol in bbg_symbols:
        exante_symbol = Asset.convert_symbol(bbg_symbol, future, bloomberg, exante)
        bar = exante_client.OHLC(exante_symbol, 24*60*60, 1, as_of, as_of)
        bars.append(bar)
    if len(bars) > 0:
        return concat(bars)
    else:
        return None


def cash_marks(positions, source=None):
    syms = symbols(positions, asset_types=[cash])
    df = DataFrame([syms], columns=['symbol'])
    df.loc[:, 'price'] = 1
    df.loc[:, 'source'] = source
    df.loc[:, 'asset_type'] = cash
    return df


def get_reyl_marks(positions, as_of: datetime):
    fx_forward_prices = get_db_prices(positions, as_of, time=None, asset_types=[fx_forward], source=reyl)
    fund_prices = get_db_prices(positions, as_of, time=None,  asset_types=[fund], source=reyl)
    fx_rates = get_gbp_exchange_rates(positions, as_of=as_of, source=reyl)
    futures_prices = exante_futures_marks(as_of, positions)
    cash_prices = cash_marks(positions, source=reyl)
    prices = concat([fund_prices, fx_forward_prices, futures_prices, cash_prices])
    prices = prices[['price', 'source', 'symbol', 'asset_type']]
    return prices, fx_rates


def get_all_marks(positions: DataFrame, as_of: datetime, mark_type=None):
    mark_map = {reyl: {fund: reyl,
                       fx_spot: reyl,
                       future: exante},
                'fair': {}}
    reyl_prices, reyl_fx_rates = get_reyl_marks(positions, as_of)
    return {reyl: [reyl_prices, reyl_fx_rates]}
