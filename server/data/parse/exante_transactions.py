from data.parse import FileParser
from app.enums import cash, exante, shiny, alex, interest, fee, transfer

from pandas import to_datetime


class ExanteTransactionParser(FileParser):
    COLUMN_MAPPING = {'Transaction ID' : 'unique',
                      'Symbol ID' : 'symbol',
                      'Operation type' : 'transaction_type',
                      'When' :'transaction_time',
                      'Sum' : 'gross_transaction_value',
                      'Asset' : 'currency',
                      'Comment' : 'description'}

    UNUSED_COLUMNS = ['Account ID']
    HEADER_ROW = 0

    MODEL_CLASS = 'Transaction'
    SOURCE = exante

    def parse(self):
        df = ExanteTransactionParser.prepare_dataframe(self.file_contents, ExanteTransactionParser.HEADER_ROW, ExanteTransactionParser.UNUSED_COLUMNS, ExanteTransactionParser.COLUMN_MAPPING)
        d = {'ROLLOVER': interest,
             'INTEREST': interest,
             'FEE' : fee,
             'FUNDING/WITHDRAWAL' : transfer}
        df = df[~df['transaction_type'].isin(['COMMISSION', 'TRADE'])].copy()
        df.loc[:, 'symbol'] = df['currency']
        df.loc[:, 'asset_type'] = cash
        df.loc[:, 'custodian'] = exante
        df.loc[:, 'owner'] = alex
        df.loc[:, 'group'] = shiny
        df.loc[:, 'asset_name'] = df['currency']
        df.loc[:, 'transaction_time'] = to_datetime(df['transaction_time'], format='%Y-%m-%d %H:%M:%S')
        df.loc[:, 'value_date'] = df['transaction_time'].dt.floor('D')
        df = FileParser.set_timezone(df, ['value_date', 'transaction_time'], 'utc')
        for k, v in d.items():
            df.loc[df['transaction_type'] == k, 'transaction_type'] = v
        df.loc[:, 'price'] = 1
        df.loc[:, 'quantity'] = df['gross_transaction_value'].astype(float)
        df.loc[:, 'tax'] = 0
        df.loc[:, 'direct_fee'] = 0
        df.loc[:, 'indirect_fee'] = 0
        df.loc[:, 'net_transaction_value'] = df['gross_transaction_value'].fillna(0.0)
        return {'Transaction': df}