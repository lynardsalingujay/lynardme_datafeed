from app.enums import reyl, one_day, fx_spot
from abc import ABC
from data.parse import FileParser
from pandas import to_datetime
from pytz import utc


class ReylFXRatesParser(FileParser, ABC):
    COLUMN_MAPPING = {'GBP': 'value'}

    UNUSED_COLUMNS = []
    HEADER_ROW = 4
    MODEL_CLASS = 'Position'
    SOURCE = reyl

    def parse(self):
        as_of = self.file_contents.iloc[1, 1]
        df = self.prepare_dataframe(self.file_contents, self.HEADER_ROW, self.UNUSED_COLUMNS, self.COLUMN_MAPPING)
        df.loc[:, 'value'] = 1 / df['value']
        df.loc[:, 'time'] = to_datetime(as_of)
        df.loc[:, 'time'] = df['time'].dt.tz_localize(utc)
        df.loc[:, 'as_of'] = df['time']
        df.loc[:, 'source'] = reyl
        df.loc[:, 'resolution'] = one_day
        df.loc[:, 'asset_type'] = fx_spot
        df.loc[:, 'symbol'] = 'GBP/' + df.iloc[:, 0]
        df.loc[:, 'symbol'] = self.convert_symbols(fx_spot, df['symbol'])
        return {'Price': df}
