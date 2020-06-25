from abc import ABC
from os.path import splitext
import hashlib

import pandas
from django.db.models import Model
from pandas import DataFrame, Series

from app.asset import Asset
from app.enums import bloomberg
from app.models import to_models, update_unique, field_names
import app.models


class FileParser(ABC):

    COLUMN_MAPPING: dict
    HEADER_ROW: int
    MODEL_CLASS: str
    SOURCE: None

    file_contents: DataFrame
    data: DataFrame

    @classmethod
    def parse_and_save(cls, file, save_new=False, update_existing=False) -> [Model]:
        parser = cls.create_parser(file)
        data = parser.parse()
        return parser.save(data, save_new=save_new, update_existing=update_existing)

    @classmethod
    def save(cls, data, save_new=False, update_existing=False):
        all_objs = []
        for model_cls_name, df in data.items():
            model_cls = getattr(app.models, model_cls_name)
            objs, fields = to_models(model_cls, df)
            if save_new:
                model_cls.objects.bulk_create(objs, ignore_conflicts=True, batch_size=5000)
            if update_existing:
                for obj in df.to_dict(orient='records'):
                    update_unique(model_cls_name, obj)
            all_objs += objs
        return all_objs, 'Successfully created ' + str(len(all_objs)) + ' models'

    @classmethod
    def create_parser(cls, file):
        file_contents = cls.extract_contents(file)
        parser_class = cls.infer_subclass(file_contents)
        obj = parser_class(file_contents)
        if isinstance(file_contents, DataFrame):
            obj.SYMBOL = file_contents.name
        return obj

    def __init__(self, file_contents):
        self.file_contents = file_contents

    @staticmethod
    def starts_with(substring, file_contents):
        l = len(substring)
        return file_contents[:l] == substring

    @classmethod
    def infer_subclass(cls, file_contents) -> type:
        from data.parse.exante_trades import ExanteTradesParser
        from data.parse.exante_transactions import ExanteTransactionParser
        from data.parse.exante_positions import ExantePositionsParser
        from data.parse.reyl_cash_movements import ReylCashMovementParser
        from data.parse.reyl_transactions import ReylTransactionParser
        from data.parse.reyl_trade_confirmations import ReylFutureConfirmParser
        from data.parse.reyl_positions import ReylPositionsParser
        from data.parse.reyl_fx_rates import ReylFXRatesParser
        from data.parse.json_model import JSONParser
        from data.parse.ohlc_data import DailyPriceParser, Monthly5mBarPriceParser
        from data.parse.static_universe import StaticUniverseParser

        def has_columns(all_columns_names, necessary_column_names):
            if len(necessary_column_names) == 0:
                return True
            else:
                return set(necessary_column_names) <= set(all_columns_names)

        json_parser_class = JSONParser.infer_subclass(file_contents)

        if json_parser_class is not None:
            return json_parser_class
        elif isinstance(file_contents, str):
            if cls.starts_with('REYL & CIE SA', file_contents):
                return ReylFutureConfirmParser
            content = file_contents.split('\n')
            parser_classes = [ExantePositionsParser]
        elif isinstance(file_contents, DataFrame):
            content = file_contents.copy()
            if file_contents.name.startswith('5m_price'):
                parser_classes = [Monthly5mBarPriceParser]
            elif file_contents.name.startswith('daily_price'):
                parser_classes = [DailyPriceParser]
            else:
                parser_classes = [ReylTransactionParser, ReylCashMovementParser, ExanteTradesParser,
                                  ExanteTransactionParser, ReylPositionsParser, ReylFXRatesParser, StaticUniverseParser]
        else:
            raise ValueError("Unknown Type of file_contents." )

        for parser_class in parser_classes:
            necessary_column_names = parser_class.COLUMN_MAPPING.keys()
            try:
                try:
                    all_column_names = content.iloc[parser_class.HEADER_ROW].to_list()
                except AttributeError:
                    all_column_names = content[parser_class.HEADER_ROW].split(',')
                except Exception as e:
                    if parser_class.HEADER_ROW is None:
                        all_column_names = content.columns.to_list()
                    else:
                        all_column_names = []

                if has_columns(all_column_names, necessary_column_names):
                    return parser_class
            except:
                raise ValueError("Could not make column comparison")

        raise ValueError("Could not infer parser class")

    @classmethod
    def check_uniques(cls, df):
        uniques = set(df['unique'].values)
        if len(uniques) < len(df.index):
            raise ValueError('unique col is not unique!')

    @classmethod
    def convert_symbols(cls, asset_type_or_types: [Series, str], symbols: Series):
        converted_symbols = Series(index=symbols.index)
        for index, symbol in symbols.items():
            if isinstance(asset_type_or_types, str):
                asset_type = asset_type_or_types
            else:
                asset_type = asset_type_or_types[index]
            converted_symbols[index] = Asset.convert_symbol(symbol, asset_type, cls.SOURCE, bloomberg)
        return converted_symbols

    @staticmethod
    def extract_contents(file):
        try:
            is_file = hasattr(file, 'name')
            if is_file:
                file_name = file.name
            else:
                file_name = file
            ext = splitext(file_name)[1]
            import os
            file_prefix = splitext(file_name)[0].split(os.sep)[-1]
            if ext in ['.xls', '.xlsx']:
                result = pandas.read_excel(file, header=None)
                result.name = file_prefix
                return result
            elif ext in ['.csv', '.txt']:
                try:
                    result = pandas.read_csv(file, header=None)
                    result.name = file_prefix
                    return result
                except Exception as e:
                    with open(file) as f:
                        contents = f.read()
                    return contents
                result = pandas.read_csv(file, header=None)
                result.name = file_prefix
                return result
            elif ext == '.pdf':
                import PyPDF2
                if not is_file:
                    file = open(file_name, 'rb')
                pdfReader = PyPDF2.PdfFileReader(file)
                count = pdfReader.numPages
                pages = ''
                for i in range(count):
                    page = pdfReader.getPage(i).extractText()
                    pages += page
                return pages
            elif ext == '.json':
                if not is_file:
                    file = open(file_name, 'rb')
                contents = file.read()
                return contents
            else:
                raise Exception("Unknown extension: " + ext)
        finally:
            if hasattr(file, 'close'):
                file.close()

    @staticmethod
    def merge_hash_id(df):
        df.loc[:, 'id'] = df.apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        df.loc[:, 'id'] = [hashlib.sha1(str.encode(combined)) for combined in df['id']]
        return df

    def prepare_contents(self):
        return self.prepare_dataframe(self.file_contents, self.HEADER_ROW, self.UNUSED_COLUMNS, self.COLUMN_MAPPING)

    @staticmethod
    def prepare_dataframe(file_contents, header_row, unused_columns, column_mapping):
        df = file_contents.copy()
        # prepare columns
        df = df.dropna(how='all', axis=1)
        df = df.transpose().drop_duplicates(subset=header_row, keep='last').transpose()
        rename_map = df.iloc[header_row].to_dict()
        df = df.rename(columns=rename_map)
        df = df.drop(unused_columns, axis=1)
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.rename(columns=column_mapping)
        # prepare rows
        df = df.iloc[header_row + 1:]
        df = df.dropna(how='all')
        df = df.drop_duplicates()
        return df

    def parse(self):
        raise NotImplementedError('This is an abstract method')


    @classmethod
    def get_or_create_model_object(cls, model_cls, fields):
        if 'unique' in fields and fields['unique'] is not None:
            try:
                object = model_cls.objects.get(unique=fields['unique'])
                for field, value in fields.items():
                    setattr(object, field, value)
                is_created = False
            except Exception:
                object, is_created = model_cls.objects.get_or_create(**fields)
        else:
            object, is_created = model_cls.objects.get_or_create(**fields)
        return object, is_created

    @staticmethod
    def set_timezone(df, cols, timezone='utc'):
        for col in cols:
            df.loc[:, col] = df[col].dt.tz_localize(timezone)
        return df
