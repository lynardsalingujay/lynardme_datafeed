from abc import ABC
from data.parse import FileParser
from app.enums import bloomberg

class StaticUniverseParser(FileParser, ABC):
    COLUMN_MAPPING = {'BB Ticker': 'bb_ticker', 'Asset class': 'asset_type', 'Contract mul': 'contract_mul',
                      'Expiry date': 'expiry_date', 'Currency': 'currency', 'Underlying Index': 'underlying_index',
                      'ISIN': 'isin', 'Name': 'name', 'Symbol': 'symbol', 'Fund size': 'fund_size',
                      'Fund size currency': 'fund_size_currency', 'Front load': 'front_load', 'Back load': 'back_load',
                      'Minimum investment': 'min_investment', 'Performance fee': 'performance_fee', 'Geo': 'geo'}


    UNUSED_COLUMNS = []
    HEADER_ROW = 0
    MODEL_CLASS = 'Universe'
    SOURCE = bloomberg
    SYMBOL = ''

    def parse(self):
        df = self.prepare_dataframe(self.file_contents, self.HEADER_ROW, self.UNUSED_COLUMNS, self.COLUMN_MAPPING)
        from pandas import to_datetime, to_numeric
        today = to_datetime('today').floor('d')
        df['as_of'] = today
        null_cols = ['contract_mul', 'expiry_date', 'underlying_index', 'isin', 'fund_size', 'fund_size_currency',
                     'front_load', 'back_load', 'min_investment', 'performance_fee', 'geo']
        numeric_cols = ['contract_mul', 'fund_size', 'front_load', 'back_load', 'min_investment', 'performance_fee']
        from pandas import to_datetime
        df.loc[:, 'expiry_date'] = to_datetime(df['expiry_date'], format='%d/%m/%Y')
        for col in numeric_cols:
            df.loc[:, col] = df[col].str.replace(',', '')
            df.loc[:, col] = df[col].str.replace(' ', '')
            df[col] = to_numeric(df[col])
        for col in null_cols:
            df[col] = df[col].astype('object')
            df[col][df[col].isnull()] = None
        return {'Universe': df}
