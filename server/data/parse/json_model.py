from data.parse import FileParser
from json import loads

from pandas import DataFrame, to_datetime, to_numeric
from pytz import utc


class JSONParser(FileParser):

    MODEL_CLASS: str

    DATE_COLS = []

    def __init__(self, file_contents):
        self.file_contents = file_contents

    @classmethod
    def create(cls, file_contents):
        parser_class = cls.infer_subclass(file_contents)
        return parser_class(file_contents)

    @classmethod
    def infer_subclass(cls, file_contents) -> type:
        meta_map = {'future_confirm': JSONFutureConfirmParser}
        class_map = {'Transaction': JSONTransactionParser,
                     'CashMovement': JSONCashMovementParser,
                     'Position': JSONPositionParser,
                     'Price': JSONPriceParser,
                     'SimResult': JSONSimResultParser}
        try:
            json = loads(file_contents)
            model, meta, _ = json['model'], json['meta'], json['data']
            parser_class = meta_map.get(json['meta'], None)
            if parser_class is None:
                return class_map[model]
            else:
                return parser_class
        except Exception as e:
            return None

    def parse(self):
        json = loads(self.file_contents)
        df = DataFrame.from_records(json['data'])
        df = self.coerce_types(df)
        return {self.MODEL_CLASS: df}

    @classmethod
    def coerce_types(cls, df):
        for col in cls.DATE_COLS:
            if col in df.columns:
                df.loc[:, col] = to_datetime(df[col]).dt.tz_localize(utc)
        return df

class JSONSimResultParser(JSONParser):
    MODEL_CLASS = 'SimResult'
    DATE_COLS = ['as_of']

class JSONTransactionParser(JSONParser):
    MODEL_CLASS = 'Transaction'
    DATE_COLS = ['transaction_time', 'value_date']


class JSONFutureConfirmParser(JSONTransactionParser):

    @classmethod
    def get_or_create_model_object(cls, model_cls, fields):
        return cls.update_future_confirm(model_cls, fields)


class JSONCashMovementParser(JSONParser):
    MODEL_CLASS = 'CashMovement'
    DATE_COLS = ['transaction_date', 'value_date']


class JSONPositionParser(JSONParser):
    MODEL_CLASS = 'Position'
    DATE_COLS = ['as_of_date', 'value_date']


class JSONPriceParser(JSONParser):
    MODEL_CLASS = 'Price'
    DATE_COLS = ['as_of', 'time']
