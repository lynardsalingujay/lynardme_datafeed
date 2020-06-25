from abc import ABC
from data.parse import FileParser
from pandas import to_datetime, concat, DataFrame, Series, MultiIndex, DatetimeIndex
from numpy import nan
from app.enums import portaracqg, five_minutes, index_future, equity_index, trade, dukascopy, fx_spot, \
    bloomberg, one_day, nav, fund, close, open, high, low


class Portaracqg5mParser(FileParser, ABC):

    COLUMN_MAPPING = {0: 'as_of', 1: 'time', 2: 'open', 3: 'high',
                      4: 'low', 5: 'close', 6: 'volume'}

    UNUSED_COLUMNS = []
    HEADER_ROW = None
    MODEL_CLASS = 'Price'
    SOURCE = portaracqg
    SYMBOL = ''

    def parse(self):
        df = self.file_contents.rename(columns=self.COLUMN_MAPPING)
        symbol_len = len(self.SYMBOL)
        dt_index = to_datetime(df['as_of'] * 10000 + df['time'], format='%Y%m%d%H%M', utc=True)
        df['as_of'] = dt_index
        df['time'] = dt_index
        df['symbol'] = self.SYMBOL
        df['source'] = self.SOURCE
        df['resolution'] = five_minutes
        if 'Index' in self.SYMBOL:
            if symbol_len == 9:
                df['asset_type'] = equity_index
            elif symbol_len == 11:
                df['asset_type'] = index_future
        else:
            raise Exception('Unknown asset type')
        df['price_type'] = trade

        aspects = ['open', 'high', 'low', 'close', 'volume']
        list_of_dfs = []
        for aspect in aspects:
            df['value'] = df[aspect]
            df['aspect'] = aspect
            temp_df = df.drop(aspects, axis='columns')
            list_of_dfs.append(temp_df)
        result = concat(list_of_dfs)
        result = result.reset_index()
        result = result.drop('index', axis='columns')
        self.data = {'Price': result}


class Dukescopy5mParser(FileParser, ABC):
    COLUMN_MAPPING = {'Gmt time': 'as_of',
                      'Open': 'open',
                      'High': 'high',
                      'Low': 'low',
                      'Close': 'close',
                      'Volume': 'volume'}

    UNUSED_COLUMNS = []
    HEADER_ROW = 0
    MODEL_CLASS = 'Price'
    SOURCE = dukascopy
    SYMBOL = ''

    def parse(self):
        df = self.prepare_dataframe(self.file_contents, self.HEADER_ROW, self.UNUSED_COLUMNS, self.COLUMN_MAPPING)
        price_type = self.SYMBOL[-3:]
        symbol = self.SYMBOL[:15]
        df['as_of'] = to_datetime(df['as_of'], format='%d.%m.%Y %H:%M:%S.000', utc=True)
        df = df.set_index('as_of')
        df = df[df.index.minute == 0]  # only save hour bar price
        df = df.reset_index()
        df['time'] = df['as_of']
        df['symbol'] = symbol
        df['source'] = self.SOURCE
        df['resolution'] = five_minutes
        df['asset_type'] = fx_spot
        df['price_type'] = price_type
        aspects = ['open', 'high', 'low', 'close', 'volume']
        list_of_dfs = []
        for aspect in aspects:
            df['value'] = df[aspect]
            df['aspect'] = aspect
            temp_df = df.drop(aspects, axis='columns')
            list_of_dfs.append(temp_df)
        result = concat(list_of_dfs)
        self.data = {'Price': result}


class DailyPriceParser(FileParser, ABC):
    COLUMN_MAPPING = {}

    UNUSED_COLUMNS = []
    HEADER_ROW = None
    MODEL_CLASS = 'Price'
    SOURCE = bloomberg
    SYMBOL = ''

    def parse(self):
        from pandas import to_datetime
        today = to_datetime('today').floor('d')
        orig_df = self.file_contents.copy()
        orig_df = orig_df.dropna(how='all', axis='index')
        orig_df.columns = orig_df.iloc[1, :]
        orig_df[nan] = to_datetime(orig_df[nan], format='%d/%m/%Y').dt.floor('s')
        df = orig_df.drop([0, 1, 2], axis='index').set_index(nan)
        asset_type_map = orig_df.iloc[2, :].dropna()
        df.index.name = 'Dates'
        cols = ['as_of', 'time', 'symbol', 'source', 'value', 'resolution', 'asset_type',
                'price_type', 'aspect']

        list_of_dfs = []
        for asset in df.columns:
            price_df = DataFrame(columns=cols)
            price_df.loc[:, 'time'] = df.index
            price_df.loc[:, 'as_of'] = today
            price_df.loc[:, 'value'] = df[asset].values
            price_df.loc[:, 'symbol'] = asset
            price_df.loc[:, 'source'] = self.SOURCE
            price_df.loc[:, 'resolution'] = one_day
            price_df.loc[:, 'asset_type'] = price_df['symbol'].map(asset_type_map)
            price_df.loc[:, 'price_type'] = trade
            price_df.loc[:, 'price_type'] = price_df['price_type'].mask(price_df['asset_type'].eq('fund'), nav)
            price_df.loc[:, 'aspect'] = close
            list_of_dfs.append(price_df)
        result = concat(list_of_dfs, ignore_index=True)
        result = result.dropna(how='any', axis='index')
        return {'Price': result}


class Monthly5mBarPriceParser(FileParser, ABC):
    COLUMN_MAPPING = {}

    UNUSED_COLUMNS = []
    HEADER_ROW = None
    MODEL_CLASS = 'Price'
    SOURCE = bloomberg
    SYMBOL = ''

    def parse(self):
        from pandas import to_datetime
        today = to_datetime('today').floor('d')
        orig_df = self.file_contents.copy()
        orig_df = orig_df.dropna(how='all', axis='index')
        orig_df.columns = orig_df.iloc[1, :]
        asset_type_map = orig_df.iloc[2, :].dropna()
        df = orig_df.drop([0, 1, 2, 3], axis='index')
        multi_index = MultiIndex.from_product([df.columns.dropna(), ['Dates', 'Open', 'Close', 'High', 'Low']])
        df.columns = multi_index
        aspects = {'Open': open, 'Close': close, 'High': high, 'Low': low}
        result_dfs = []
        cols = ['as_of', 'time', 'symbol', 'source', 'value', 'resolution', 'asset_type',
                'price_type', 'aspect']
        for asset, price_df in df.groupby(level=0, axis='columns'):
            price_df = price_df[asset].copy()
            price_df['Dates'] = to_datetime(price_df['Dates'], format='%d/%m/%Y %H:%M').dt.floor('s')
            price_df = price_df.set_index('Dates')
            price_df.index = price_df.index.tz_localize('Europe/London')
            price_df.index = price_df.index.tz_convert('UTC')
            price_df = price_df.dropna(how='all')
            list_of_aspect_price_dfs = []
            for aspect_name, aspect in aspects.items():
                new_price_df = DataFrame(columns=cols)
                new_price_df.loc[:, 'time'] = price_df.index
                new_price_df.loc[:, 'as_of'] = today
                new_price_df.loc[:, 'value'] = price_df[aspect_name].values
                new_price_df.loc[:, 'symbol'] = asset
                new_price_df.loc[:, 'source'] = self.SOURCE
                new_price_df.loc[:, 'resolution'] = five_minutes
                new_price_df.loc[:, 'asset_type'] = asset_type_map[asset]
                new_price_df.loc[:, 'price_type'] = trade
                new_price_df.loc[:, 'aspect'] = aspect
                list_of_aspect_price_dfs.append(new_price_df)
            result_df = concat(list_of_aspect_price_dfs, ignore_index=True)
            result_dfs.append(result_df)
        result = concat(result_dfs, ignore_index=True)
        result = result.dropna(how='any', axis='index')
        return {'Price': result}

