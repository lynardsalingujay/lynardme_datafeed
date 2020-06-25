from pandas import DataFrame

from data.http.ft import parse_daily as ft_parse_daily

parser_map = {('ft', 'daily'): ft_parse_daily}


def add_new_column(df, name, value):
    if not name in df.columns:
        df.loc[:, name] = value
    return df


def merge_request_fields(symbol, resolution, source, security_type, df: DataFrame):
    from pandas import to_numeric
    df = add_new_column(df, 'Source', source)
    df = add_new_column(df, 'Symbol', symbol)
    df = add_new_column(df, 'Resolution', resolution)
    df = add_new_column(df, 'Type', security_type)
    df.loc[:, 'Price'] = to_numeric(df['Price'])
    return df


def _dispatch_parse_prices(resolution, source):
    try:
        return parser_map[(source, resolution)]
    except:
        s = 'no parser for {resolution} / {source}'
        s = s.format(resolution=resolution, source=source)
        raise Exception(s)


def parse_prices(symbol, resolution, source, _type):
    parser = _dispatch_parse_prices(resolution, source)
    df = parser(symbol)
    data = merge_request_fields(symbol, resolution, source, _type, df)
    return data


if __name__ == "__main__":
    x = parse_prices("FR0007017488", "daily", "ft", "fund")
    print(x)