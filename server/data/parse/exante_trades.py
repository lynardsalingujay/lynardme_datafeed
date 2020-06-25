from data.parse import FileParser
from app.enums import exante, index_future, fx_spot, alex, shiny, fx_future

from pandas import to_datetime


class ExanteTradesParser(FileParser):

    COLUMN_MAPPING = {'Time': 'transaction_time',
                      'Side': 'side',
                      'Symbol ID': 'symbol',
                      'Type': 'asset_type',
                      'Price': 'price',
                      'Currency': 'currency',
                      'Quantity': 'quantity',
                      'Commission': 'direct_fee',
                      'Traded Volume': 'gross_transaction_value',
                      'Order Id': 'unique',
                      'Value date': 'value_date'}

    HEADER_ROW = 0
    UNUSED_COLUMNS = []
    MODEL_CLASS = 'Transaction'
    SOURCE = exante

    def parse(self):
        df = ExanteTradesParser.prepare_dataframe(self.file_contents, ExanteTradesParser.HEADER_ROW,
                                                          ExanteTradesParser.UNUSED_COLUMNS,
                                                          ExanteTradesParser.COLUMN_MAPPING)

        df = df[~df['transaction_time'].isnull() & ~df['value_date'].isnull()].copy()

        fx_future_index = df['asset_type'] == 'FX_SPOT'
        future_index = df['asset_type'] == 'FUTURE'
        fx_index = df['asset_type'] == 'FOREX'
        
        df.loc[:, 'custodian'] = exante
        df.loc[:, 'owner'] = alex
        df.loc[:, 'group'] = shiny

        df.loc[:, 'transaction_time'] = to_datetime(df['transaction_time'], format='%Y-%m-%d %H:%M:%S')
        df.loc[:, 'value_date'] = to_datetime(df['value_date'], format='%Y-%m-%d')
        df = FileParser.set_timezone(df, ['value_date', 'transaction_time'], 'utc')
        df.loc[fx_index, 'symbol'] = ExanteTradesParser.convert_symbols(fx_spot, df.loc[fx_index, 'symbol'])
        df.loc[fx_future_index, 'symbol'] = ExanteTradesParser.convert_symbols(fx_spot, df.loc[fx_future_index, 'symbol'])
        df.loc[future_index, 'symbol'] = ExanteTradesParser.convert_symbols(index_future, df.loc[future_index, 'symbol'])
        df.loc[fx_index, 'asset_type'] = fx_spot
        df.loc[fx_future_index, 'asset_type'] = fx_future
        df.loc[future_index, 'asset_type'] = index_future
        df.loc[:, 'asset_name'] = df['symbol']
        df.loc[:, 'transaction_type'] = df['side']
        df.loc[:, 'price'] = df['price'].fillna(0.0).astype(float)
        df.loc[:, 'quantity'] = df['quantity'].fillna(0.0).astype(float)
        df.loc[:, 'tax'] = 0
        df.loc[:, 'direct_fee'] = - df['direct_fee'].fillna(0.0).astype(float).abs()
        df.loc[:, 'indirect_fee'] = 0.0
        df.loc[:, 'gross_transaction_value'] = df['gross_transaction_value'].fillna(0.0).astype(float)
        df.loc[:, 'net_transaction_value'] = df.loc[:, 'gross_transaction_value'] + df.loc[:, 'direct_fee']
        df.loc[:, 'description'] = None
        return {'Transaction': df}

