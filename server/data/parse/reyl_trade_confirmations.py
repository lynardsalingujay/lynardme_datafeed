from data.parse import FileParser
from app.enums import reyl, bloomberg, index_future, sell, buy, mid_pacific_am, mft
from app.asset import Asset

from datetime import datetime

from pandas import DataFrame
from pytz import utc


class ReylFutureConfirmParser(FileParser):


    OPEN_MAP = {'We ': ['quantity'],
                'Order date': ['transaction_time'],
                'Price': [None, 'price', 'currency'],
                'Broker commission': [None, 'indirect_fee'],
                'Commission': [None, 'direct_fee'],
                'Value date:': ['value_date'],
                'Securities transaction': ['transaction_type', None, 'symbol']}

    CLOSE_MAP = {'We ': ['quantity'],
                'Order date': ['transaction_time'],
                'Closing': [None, None, None, None, 'price', 'currency'],
                'Broker commission': [None, 'indirect_fee'],
                'Commission': [None, 'direct_fee'],
                'Value date:': ['value_date'],
                'Securities transaction': ['transaction_type', None, 'symbol']}

    MODEL_CLASS = 'Transaction'

    @classmethod
    def save(cls, data, save_new=False, update_existing=False):
        from app.models import update_unique
        unique_fields = ['asset_type', 'transaction_type', 'quantity', 'value_date',
                         'custodian', 'owner', 'group', 'symbol']
        # update_fields = ['price', 'gross_transaction_value', 'net_transaction_value']
        d = data['Transaction'].to_dict(orient='records')[0]
        return update_unique('Transaction', d, unique_fields=unique_fields)

    @staticmethod
    def find_fragment(frag, lines):
        for i, line in enumerate(lines):
            if line[:len(frag)] == frag:
                return i
        return None

    @classmethod
    def select_fragment_map(cls, lines):
        i = cls.find_fragment('Securities transaction', lines)
        key = lines[i].split(' ')[-1]
        if key.lower() == 'close':
            return cls.CLOSE_MAP
        elif key.lower() == 'open':
            return cls.OPEN_MAP
        else:
            raise ValueError('could not find open/close')

    @classmethod
    def extract_fields_where_no_lines(cls, raw):
        frags = ['Securities transaction', 'We ', ' for your account on ', ' - to ', ' -', 'Future', 'Security:', 'Market place:',
                 'Contract value', 'Last trading date', 'ContractPrice', 'Broker commission', 'Commission', 'Net', 'Average price:',
                 'Initial Margin', 'Value date:', 'Page 1']
        frag_map = dict()
        d = dict()
        for frag_1, frag_2 in zip(frags[:-1], frags[1:]):
            splits = raw.split(frag_2)
            value = splits[0].split(frag_1)[-1]
            rest = ''.join([frag_2] + splits[1:])
            frag_map[frag_1] = value
        d['transaction_type'] = frag_map['Securities transaction']
        d['symbol'] = frag_map['Future']
        d['price'] = frag_map['Average price:']
        d['currency'] = frag_map['Broker commission'][:3]
        d['indirect_fee'] = frag_map['Broker commission'][4:]
        d['direct_fee'] = frag_map['Commission'][4:]
        d['value_date'] = frag_map['Value date:']
        d['quantity'] = frag_map[' -']
        d['transaction_time'] = frag_map[' for your account on ']
        return d

    @classmethod
    def find_average_price(cls, lines):
        frag = 'Average price:'
        i = cls.find_fragment(frag, lines)
        if i:
            text = lines[i]
            text = text.replace(frag, '')
            return text

    @classmethod
    def extract_fields_where_lines(cls, raw):
        lines = raw.split('\n')
        field_map = dict()
        frag_map = cls.select_fragment_map(lines)
        for fragment, fields in frag_map.items():
            i = cls.find_fragment(fragment, lines)
            if i:
                for n, field in enumerate(fields):
                    if field:
                        value = lines[n + i + 1]
                        field_map[field] = value
        av_price = cls.find_average_price(lines)
        if av_price:
            field_map['price'] = av_price
        return field_map

    @classmethod
    def extract_fields(cls, raw):
        if "\n" in raw:
            return cls.extract_fields_where_lines(raw)
        else:
            return cls.extract_fields_where_no_lines(raw)

    @staticmethod
    def parse_number(n):
        n = n.replace("'", "")
        n = n.replace(" ", "")
        n = float(n)
        return n

    @staticmethod
    def parse_date(date):
        date = datetime.strptime(date, '%d.%m.%Y')
        date = utc.localize(date)
        return date

    @staticmethod
    def parse_symbol(text):
        splits = text.split('(')
        symbol = splits[1].split(')')[0]
        symbol = Asset.convert_symbol(symbol, index_future, reyl, bloomberg)
        mult = splits[-1].split(',')[0].replace('Size ', '')
        return symbol, float(mult)

    @staticmethod
    def parse_transaction_type(text):
        sell_words = ['sold', 'sale']
        buy_words = ['bought', 'buy', 'purchased', 'purchase']
        l_text = text.lower()
        for word in sell_words:
            if word in l_text:
                return sell
        for word in buy_words:
            if word in l_text:
                return buy
        raise ValueError('cannot parse transaction type')

    def parse(self):
        d = self.extract_fields(self.file_contents)
        # parse the fields
        d['transaction_type'] = self.parse_transaction_type(d['transaction_type'])
        if d['transaction_type'] == sell:
            d['quantity'] = -float(d['quantity'])
        else:
            d['quantity'] = float(d['quantity'])

        d['price'] = self.parse_number(d['price'])
        d['symbol'], mult = self.parse_symbol(d['symbol'])
        d['indirect_fee'] = -abs(self.parse_number(d['indirect_fee']))
        d['direct_fee'] = -abs(self.parse_number(d['direct_fee']))
        d['value_date'] = self.parse_date(d['value_date'])
        d['transaction_time'] = self.parse_date(d['transaction_time'])
        # constant fields
        d['asset_type'] = index_future
        d['custodian'] = reyl
        d['owner'] = mid_pacific_am
        d['group'] = mft
        d['gross_transaction_value'] = d['quantity'] * mult * d['price']
        d['net_transaction_value'] = d['gross_transaction_value'] - d['indirect_fee'] - d['direct_fee']
        df = DataFrame(data=[list(d.values())], columns=list(d.keys()))
        return {'Transaction': df}

