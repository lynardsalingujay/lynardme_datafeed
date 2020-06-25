from datetime import datetime

from pandas import DataFrame

from app.enums import bloomberg, reyl, exante, index_future, fx_spot, fx_forward, fund, cash


class Asset:

    @classmethod
    def convert_symbol(cls, symbol_in, asset_type=None, source_in=None, source_out=bloomberg):
        sec = cls.create_asset(symbol_in, asset_type, source_in)
        return sec.to_symbol(source_out)

    @classmethod
    def create_asset(cls, symbol, asset_type=None, source=None):
        cls_map = {index_future: Future,
                   fx_spot: FX,
                   fx_forward: FX,
                   fund: Fund,
                   cash: Cash}
        if asset_type in cls_map:
            return cls_map[asset_type](symbol, source)
        else:
            for _cls in cls_map.values():
                try:
                    _cls(symbol, source=source)
                except:
                    pass
        raise TypeError('could not find suitable constructor')

    def to_symbol(self, source):
        raise NotImplementedError()


class Cash(Asset):

    def __init__(self, symbol, source=None):
        self.symbol = symbol

    def to_symbol(self, source):
        return self.symbol


class Future(Asset):

    CONTRACT_MULTS = DataFrame({'exante_group':     ['SPX', 'TOPIX', 'HSI', 'RTY', 'N225', 'NaN', 'NaN', 'NaN'],
                                'bloomberg_group':  ['SPX', 'TPX', 'HSI', 'RTY', 'NKY', 'NaN', 'NaN', 'NaN'],
                                'bloomberg_stub':   ['ES', 'TP', 'HI', 'RTY', 'NK', 'NI', 'NaN', 'NaN'],
                                'exante_stub':      ['ES.CME.', 'TOPIX.OE.', 'HSI.HKEX.', 'RTY.CME.', 'JN4F.OE.', 'NK225M.OE.', 'NK225.OE.', 'MES.CME.'],
                                'mult':             [50, 10000, 50, 50, 1000, 1, 1, 1]})

    CONTRACT_MONTHS = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}
    BLOOMBERG_SUFFIX = " Index"

    _symbol_stub: str
    _symbol_stub_source: str

    month_letter: str
    year: int

    @staticmethod
    def extract_year_digits(symbol_name):
        for nlen in [2, 1]:
            try:
                return int(symbol_name[-nlen:])
            except:
                pass

    @staticmethod
    def as_full_year(year):
        if year < 10:
            this_year = datetime.today().year - 9
            this_decade = int(this_year/10)*10
            year = this_decade + year
        elif year < 100:
            this_year = datetime.today().year
            this_century = int(this_year/100)*100
            year = this_century + year
        else:
            raise ValueError('future year expected to be less than 100')
        return year

    @staticmethod
    def deconstruct_bloomberg_name(name):
        year = Future.extract_year_digits(name)
        name, _ = name.split(str(year))
        year = Future.as_full_year(year)
        month = name[-1]
        group = name[:-1]
        return group, month, year

    @staticmethod
    def deconstruct_bloomberg_symbol(symbol):
        name, _ = symbol.split(Future.BLOOMBERG_SUFFIX)
        return Future.deconstruct_bloomberg_name(name)

    def __init__(self, symbol, source=None):
        self._symbol_stub_source = source
        if source == bloomberg:
            self._symbol_stub, self.month_letter, self.year = self.deconstruct_bloomberg_symbol(symbol)
        elif source == reyl:
            if len(symbol.split(' ')) > 1:
                self._symbol_stub, self.month_letter, self.year = self.deconstruct_reyl_long_name(symbol)
            else:
                self._symbol_stub, self.month_letter, self.year = self.deconstruct_bloomberg_name(symbol)
        elif source == exante:
            self._symbol_stub, self.month_letter, self.year = self.deconstruct_exante_name(symbol)
        else:
            raise NotImplementedError('Cannot parse futures symbols for ' + source)

    def contract_mult(self):
        df = self.CONTRACT_MULTS
        col = self._symbol_stub_source.lower() + '_stub'
        mult = df.loc[(df[col] == self._symbol_stub), 'mult'].iloc[0]
        return mult

    def convert_stubs(self, stub_in, source_in, source_out):
        suffix = '_stub'
        column_in = source_in.lower() + suffix
        column_out = source_out.lower() + suffix
        try:
            return self.CONTRACT_MULTS.loc[self.CONTRACT_MULTS[column_in] == stub_in, column_out].iloc[0]
        except Exception as e:
            return 'NaN'

        #except IndexError:
        #    raise IndexError('"{stub_in}" is not in column "{column_in}" of Dataframe "{class_dataframe}"'.format(stub_in=stub_in, column_in=column_in, class_dataframe='CONTRACT_MULTS'))
        #except KeyError:
        #    raise KeyError('Column "{column_in}" or column "{column_out}" is not in Dataframe "{class_dataframe}"'.format(column_in=column_in,column_out=column_out,class_dataframe='CONTRACT_MULTS'))



    def symbol_stub(self, source):
        compatible_sources = [reyl, bloomberg]
        if source == self._symbol_stub_source:
            return self._symbol_stub
        elif source in compatible_sources and self._symbol_stub_source in compatible_sources:
            return self._symbol_stub
        else:
            return self.convert_stubs(self._symbol_stub, self._symbol_stub_source, source)

    def bloomberg_year(self):
        if datetime.today().year <= self.year:
            return str(self.year)[-1]
        else:
            return str(self.year)[-2:]

    def to_symbol(self, source):
        stub = self.symbol_stub(source)
        if source == bloomberg:
            return stub + str(self.month_letter) + self.bloomberg_year() + Future.BLOOMBERG_SUFFIX
        elif source == reyl:
            return stub + str(self.month_letter) + self.bloomberg_year()
        elif source == exante:
            return stub + str(self.month_letter) + str(self.year)
        else:
            raise NotImplementedError('Cannot export symbol for source=' + source)

    @classmethod
    def deconstruct_reyl_long_name(cls, symbol):
        fragments = symbol.split(' ')
        index_name, expiration = fragments[0], fragments[-1]
        if index_name.lower() == 'topix':
            stub = 'TP'
        elif index_name.lower() == 'nikkei':
            stub = 'NH'
        else:
            raise ValueError('Cannot parse symbol='+symbol)
        month, year = expiration.split('.')
        month_letter = cls.CONTRACT_MONTHS[int(month)]
        return stub, month_letter, int(year)


    @classmethod
    def deconstruct_exante_name(cls, symbol):
        ticker, exch, month_year = symbol.split('.')
        stub = '.'.join( [ticker.upper(), exch.upper(), ''] )
        month_letter = month_year[0]
        year = int(month_year[1:])
        return stub, month_letter, year


class FX(Asset):

    BLOOMBERG_SUFFIX = " Curncy"

    EXANTE_SUFFIX = '.E.FX'
    numerator: str
    denominator: str

    PRECEDENCE = {'GBP': 100, 'EUR': 90, 'USD': 80, 'JPY': 70}

    currencies = list(PRECEDENCE.keys())

    def __init__(self, symbol, source=None):
        if source == bloomberg:
            symbol, _ = symbol.split(FX.BLOOMBERG_SUFFIX)
            if len(symbol) != 6:
                raise TypeError(symbol + ' is not a valid crossrate')
            self.numerator = symbol[1:3]
            self.denominator = symbol[4:6]
        elif source == exante:
            temp = symbol.split('.')
            symbol = temp[0]
            self.numerator, self.denominator = symbol.split('/')
        elif source in [reyl]:
            self.numerator, self.denominator = symbol.split('/')
        else:
            raise NotImplementedError('Cannot parse fx symbols for source=' + source)
        self.correct_upside_down_currencies()

    def correct_upside_down_currencies(self):
        n = self.PRECEDENCE.get(self.numerator, 0)
        d = self.PRECEDENCE.get(self.denominator, 0)
        if n < d:
            self.denominator, self.numerator = self.numerator, self.denominator

    def to_symbol(self, source):
        if source == bloomberg:
            return self.numerator + self.denominator + self.BLOOMBERG_SUFFIX
        elif source == reyl:
            return self.numerator + '/' + self.denominator
        elif source == exante:
            return self.to_symbol(reyl) + self.EXANTE_SUFFIX
        else:
            return self.numerator + '/' + self.denominator


class Fund(Asset):

    BLOOMBERG_SUFFIX = ' ISIN'

    isin: str

    def __init__(self, symbol, source=None):
        if source == bloomberg:
            self.isin, _ = symbol.split(self.BLOOMBERG_SUFFIX)
        elif source == reyl:
            self.isin = symbol
        else:
            raise NotImplementedError('Cannot parse fund symbols for source=' + source)

    def to_symbol(self, source):
        if source == bloomberg:
            return self.isin + self.BLOOMBERG_SUFFIX
        elif source == reyl:
            return self.isin
        else:
            raise NotImplementedError('Cannot generate fund symbols for source=' + source)


if __name__ == '__main__':
    future = Future('RTYU18 Index', bloomberg)
    symbol = future.to_symbol(bloomberg)
    print(symbol)