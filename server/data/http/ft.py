import requests
from lxml.html import fromstring, tostring
from pandas import read_html

ticker_to_symbol = {'SMX Index': 'SMC:FSI',
                    'MCX Index': 'FTSM:FSI',
                    'INDEXEURO:CACMD': 'CACMD:PAR',
                    'CS90 Index': 'CACS:PAR'}


def url(symbol, currency, security_type):
    if security_type == "index":
        symbol = ticker_to_symbol[symbol]
    elif security_type == "fund":
        symbol = symbol + ":" + currency
    return 'https://markets.ft.com/data/indices/tearsheet/historical?s=' + symbol


def parse_table(df):
    from pandas import to_datetime, to_numeric
    df.loc[:, 'split'] = df['Date'].str.split(',')
    df.loc[:, 'string'] = df['split'].str[-2] + df['split'].str[-1]
    df.loc[:, 'Date'] = to_datetime(df['string'], format=' %b %d %Y')
    df.loc[:, 'Close'] = to_numeric(df['Close'])
    df = df.drop(['split', 'string'], axis=1).rename(columns={'Close': 'Price', 'Date': 'Time'})
    df = df.set_index('Time')
    df.index = df.index.tz_localize('UTC')
    return df.reset_index()

    
def extract_table(page):
    tree = fromstring(page.content)
    table = tree.xpath('//table')[0]
    df = read_html(tostring(table))[0]
    return df


def parse_daily(symbol, currency, security_type):
    url_ = url(symbol, currency, security_type)
    page = requests.get(url_)
    df = extract_table(page)
    df = parse_table(df)
    return df[['Time', 'Price']]
