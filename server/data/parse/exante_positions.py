from data.parse import FileParser
from app.enums import exante, cash, index_future, fx_spot, alex, shiny, fx_future

from re import search
from datetime import datetime
import pandas as pd
from io import StringIO
import re


class ExantePositionsParser(FileParser):
    COLUMN_MAPPING = {'Account' :0,
                      'Date' : 1,
                      'Account Value' : 2,
                      'Available' : 3,
                      'Used for Margin' : 4,
                      'Margin Utilization' : 5}

    UNUSED_COLUMNS = []
    HEADER_ROW = 1
    MODEL_CLASS = 'Position'
    SOURCE = exante


    @classmethod
    def extract_as_of_date(cls, line):
        as_of_date_str = search('[0-9]{4}-[0-9]{2}-[0-9]{2}', line).group()
        as_of_date = datetime.strptime(as_of_date_str, '%Y-%m-%d')
        return as_of_date

    @classmethod
    def parse_cash(cls, df):
        cols = { 'ISO' : 'currency', 'Value':'quantity'}
        pos = df.copy()
        pos = pos[list(cols.keys())]
        pos.rename(columns=cols, inplace=True)
        pos.loc[:, 'symbol'] = pos['currency']
        pos.loc[:, 'asset_type'] = cash
        return pos

    @classmethod
    def parse_fx(cls,df):
        cols = {'Instrument': 'symbol',
                'QTY': 'quantity',
                'CCY': 'currency'}
        pos = df.copy()
        pos = pos[list(cols.keys())]
        pos.rename(columns=cols, inplace=True)
        pos.loc[:, 'symbol'] = ExantePositionsParser.convert_symbols(fx_spot, pos.loc[:, 'symbol'])
        pos.loc[:, 'asset_type'] = fx_future
        return pos

    @classmethod
    def parse_future(cls, df):
        cols = {'Instrument': 'symbol',
                'QTY': 'quantity',
                'CCY': 'currency'}
        pos = df.copy()
        pos = pos[list(cols.keys())]
        pos.rename(columns=cols, inplace=True)
        pos.loc[:, 'symbol'] = ExantePositionsParser.convert_symbols(index_future, pos.loc[:, 'symbol'])
        pos.loc[:, 'asset_type'] = index_future
        return pos

    @classmethod
    def to_dataframe(cls, lines):
        context = '\n'.join(lines)
        return pd.read_csv(StringIO(context))

    def parse(self):
        groups = self.file_contents.split('\n\n')

        df = pd.DataFrame()

        for grp in groups:
            subgrp = grp.strip().split('\n')
            if len(subgrp) > 1 and re.match('"General', subgrp[0]) == None:
                as_of_date = ExantePositionsParser.extract_as_of_date(subgrp[0])
                pos = ExantePositionsParser.to_dataframe(subgrp[1:])
                if re.match('"Cash Balance', subgrp[0]):
                    pos = ExantePositionsParser.parse_cash(pos)
                elif re.match('"Forex', subgrp[0]):
                    pos = ExantePositionsParser.parse_fx(pos)
                elif re.match('"Future', subgrp[0]):
                    pos = ExantePositionsParser.parse_future(pos)
                else:
                    raise NotImplemented("It has not been implemented for this type!")
                pos['as_of_date'] = as_of_date
                df = pd.concat([df,pos])

        df.loc[:, 'custodian'] = exante
        df.loc[:, 'owner'] = alex
        df.loc[:, 'group'] = shiny
        df.loc[:, 'value_date'] = None
        df = FileParser.set_timezone(df, ['as_of_date'], 'utc')
        df.loc[:, 'unique'] = None
        return {'Position': df}