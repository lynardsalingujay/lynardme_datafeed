from abc import ABC

from data.parse import FileParser
from app.enums import reyl, mid_pacific_am, mft, cash, fund, index_future, fx_forward, one_day
from pandas import Series, to_datetime
from pytz import utc


class ReylPositionsParser(FileParser, ABC):

    COLUMN_MAPPING = {'Quantity/ Capital called / Contracts': 'quantity',
                      'Maturity date': 'value_date',
                      'Currency': 'currency',
                      'Asset grouping': 'asset_type',
                      'Account number/ ISIN': 'symbol',
                      'Description': 'Description'}

    ASSET_TYPE_MAPPING = {'Current Accounts': cash,
                      'Equity Funds': fund,
                      'Thematic Equity Funds': fund,
                      'Options/Futures': index_future,
                      'Forward Exchange': fx_forward}

    PRICE_MAPPING = {'Market price (asset CCY)': 'value',
                     'Last price / valuation date': 'time'}

    UNUSED_COLUMNS = []
    HEADER_ROW = 7
    MODEL_CLASS = 'Position'
    SOURCE = reyl

    @staticmethod
    def parse_future_symbol(s: Series) -> Series:
        s = s.str.split('(')
        s = s.str[-1]
        s = s.str.split(')')
        s = s.str[0]
        return s

    @staticmethod
    def parse_fx_forward_symbol(s: Series) -> Series:
        s = s.str.replace('Your sale ', '').str.replace('Your purchase ', '')
        s = s.str.split(' ').str[0]
        return s

    @classmethod
    def drop_first_descrition_col(cls, df):
        def find_col(row):
            for col, value in row.items():
                if value == 'Description':
                    return col
        row = df.iloc[cls.HEADER_ROW]
        col = find_col(row)
        df = df.drop(col, axis=1)
        return df

    def parse(self):
        as_of = self.file_contents.iloc[4, 1]
        df = self.prepare_dataframe(self.file_contents, self.HEADER_ROW, self.UNUSED_COLUMNS, self.COLUMN_MAPPING)
        p = df.copy()
        p = p[list(self.PRICE_MAPPING.keys())].rename(columns=self.PRICE_MAPPING)
        df = df[list(self.COLUMN_MAPPING.values())]
        df['custodian'] = reyl
        df['owner'] = mid_pacific_am
        df['group'] = mft
        df['as_of_date'] = to_datetime(as_of)
        df.loc[:, 'as_of_date'] = df['as_of_date'].dt.tz_localize(utc)
        df.loc[:, 'asset_type'] = df['asset_type'].map(self.ASSET_TYPE_MAPPING)
        df.loc[df['asset_type'] == index_future, 'symbol'] = self.parse_future_symbol(df['Description'])
        df.loc[df['asset_type'] == fx_forward, 'symbol'] = self.parse_fx_forward_symbol(df['Description'])
        df.loc[df['asset_type'] == cash, 'symbol'] = df['Description'].str.split(' ').str[-1]
        df.loc[:, 'symbol'] = self.convert_symbols(df['asset_type'], df['symbol'])
        null_value_date_index = ~df['value_date'].isnull()
        df.loc[null_value_date_index, 'value_date'] = to_datetime(df['value_date']).dt.tz_localize(utc)
        df.loc[df['value_date'].isnull(), 'value_date'] = None
        p.loc[:, 'source'] = reyl
        p.loc[:, 'resolution'] = one_day
        p.loc[:, 'symbol'] = df['symbol']
        p.loc[:, 'asset_type'] = df['asset_type']
        p.loc[:, 'time'] = to_datetime(p['time']).dt.tz_localize(utc)
        p.loc[:, 'as_of'] = df['as_of_date']
        p = p[~p['value'].isin([0, 1])]
        return {'Position': df, 'Price': p}
