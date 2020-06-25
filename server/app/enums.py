from enum import Enum, EnumMeta


class ChoiceEnum(Enum):
    @classmethod
    def choices(cls):
        return tuple((i.value, i.value) for i in cls)


class AssetType(ChoiceEnum):
    unknown = 'unknown'
    index_future = 'index_future'
    fund = 'fund'
    cash = 'cash'
    fx_spot = 'fx_spot'
    cash_equity = 'cash_equity'
    fx_forward = 'fx_forward'
    fx_future = 'fx_future'
    equity_index = 'equity_index'
    volatility_index = 'volatility_index'


class TransactionType(ChoiceEnum):
    unknown = 'unknown'
    buy = 'buy'
    sell = 'sell'
    interest = 'interest'
    dividend = 'dividend'
    fee = 'fee'
    transfer = 'transfer'


class Source(ChoiceEnum):
    unknown = 'unknown'
    bloomberg = 'Bloomberg'
    exante = 'Exante'
    reyl = 'Reyl'
    ft = 'Ft'
    portaracqg = 'PortaraCQG'
    dukascopy = 'Dukascopy'


class Resolution(ChoiceEnum):
    unknown = 'unknown'
    one_second = '1s'
    one_minute = '1m'
    one_hour = '1h'
    one_day = '1d'
    five_minutes = '5m'


class Aspect(ChoiceEnum):
    close = 'close'
    open = 'open'
    high = 'high'
    low = 'low'
    volume = 'volume'


class PriceType(ChoiceEnum):
    unknown = 'unknown'
    trade = 'trade'
    bid = 'bid'
    ask = 'ask'
    nav = 'nav'
    mid = 'mid'


class Custodian(ChoiceEnum):
    unknown = 'unknown'
    reyl = 'Reyl'
    exante = 'Exante'
    ubs = 'UBS'
    selftrade = 'Selftrade'
    interactive_brokers = 'Interactive Brokers'


class Owner(ChoiceEnum):
    unknown = 'unknown'
    shiny = 'Shiny'
    alex = 'Alex'
    ed = 'Ed'
    mid_pacific_am = 'Mid Pacific AM'


class Group(ChoiceEnum):
    unknown = 'unknown'
    shiny = 'Shiny'
    aviva = 'Aviva'
    mft = 'MFT'


def enum_metas():
    from sys import modules
    from inspect import getmembers
    module = modules[__name__]
    members = getmembers(module)
    for member in members:
        if isinstance(member, tuple):
            if isinstance(member[1], EnumMeta):
                for meta in member[1]:
                    yield module, meta.name, meta.value


def export_names():
    for module, name, value in enum_metas():
        setattr(module, name, value)


export_names()
