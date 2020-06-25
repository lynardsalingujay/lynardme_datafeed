from app.enums import reyl, mft, mid_pacific_am
from data.parse import FileParser


class ReylCashMovementParser(FileParser):

    COLUMN_MAPPING = {'Trade date': 'transaction_date',
                      'Value date': 'value_date',
                      'Description': 'description',
                      'Debit': 'debit_amount',
                      'Credit': 'credit_amount',
                      'Balance': 'balance'}

    UNUSED_COLUMNS = []

    HEADER_ROW = 5

    MODEL_CLASS = 'CashMovement'

    SOURCE = reyl

    @classmethod
    def find_currency(cls, raw):
        text = raw.iloc[2, 1]
        currency = text[:3]
        return currency

    def parse(self):
        df = ReylCashMovementParser.prepare_dataframe(self.file_contents, ReylCashMovementParser.HEADER_ROW,
                                                      ReylCashMovementParser.UNUSED_COLUMNS,
                                                      ReylCashMovementParser.COLUMN_MAPPING)
        currency = ReylCashMovementParser.find_currency(self.file_contents)
        df = df[~df['transaction_date'].isnull() & ~df['value_date'].isnull()].copy()
        df.loc[:, 'currency'] = currency
        df.loc[:, 'credit_amount'] = df['credit_amount'].fillna(0.0)
        df.loc[:, 'debit_amount'] = df['debit_amount'].fillna(0.0)
        df.loc[:, 'transaction_date'] = df['transaction_date'].dt.to_pydatetime()
        df.loc[:, 'value_date'] = df['value_date'].dt.to_pydatetime()
        df.loc[:, 'transaction_date'] = df['transaction_date'].dt.tz_localize('UTC')
        df.loc[:, 'value_date'] = df['value_date'].dt.tz_localize('UTC')
        df.loc[:, 'custodian'] = reyl
        df.loc[:, 'owner'] = mid_pacific_am
        df.loc[:, 'group'] = mft
        df.loc[:, 'unique'] = None
        return {'CashMovement': df}
