from collections import OrderedDict, defaultdict

from pandas import Series, merge, to_numeric
from app.enums import buy, sell, fund, fx_forward, index_future, fx_spot, reyl, cash


class TradeMatcher():
    TEST_SIZE = 100000

    CONVERSION_SIZE = 10000

    TEST_UNIQUES = ['ABR223162', 'ABR223413', 'K20190821/96236/GBP',
                    'ABR225164', 'ABR225166', 'K20190904/96827/GBP']

    def __init__(self):
        self.open_symbol_map = dict()
        self.open_geo_map = dict()
        self.close_map = dict()
        self.trade_size_map = defaultdict(float)
        self.fund_symbol_map = dict()
        self.fund_open_date_map = dict()
        self.fund_close_date_map = dict()

    def guess_open_trade_number(self, row):
        if row['trade_number'] is not None:
            return row['trade_number']
        roll_key = (row['date'], row['geography'], row['asset_type'])
        if roll_key in self.close_map:
            return self.if_plausible_size(row, self.close_map[roll_key])
        if row['symbol'] in self.open_symbol_map:
            trade_number_map = self.open_symbol_map[row['symbol']]
            if len(trade_number_map.keys()) > 0:
                latest = max(trade_number_map.keys())
                return self.if_plausible_size(row, latest)
        if row['geography'] in self.open_geo_map and row['asset_type'] == index_future:
            return self.if_plausible_size(row, self.open_geo_map[row['geography']])

    def guess_close_trade_number(self, row):
        if row['trade_number'] is not None:
            return row['trade_number']
        if (row['date'], row['geography']) in self.fund_close_date_map:
            return self.fund_close_date_map[(row['date'], row['geography'])]

    def update_open_map(self, row, trade_number):
        if trade_number is None:
            raise ValueError('cannot update map with null trade number')
        self.open_geo_map[row['geography']] = trade_number
        sym = row['symbol']
        if sym not in self.open_symbol_map:
            self.open_symbol_map[sym] = OrderedDict()
        date_map = self.open_symbol_map[sym]
        if trade_number not in date_map:
            date_map[trade_number] = 0
        date_map[trade_number] += row['quantity']
        if date_map[trade_number] == 0:
            del date_map[trade_number]

    def update_close_map(self, row, trade_number):
        self.close_map[(row['date'], row['geography'])] = trade_number

    def if_plausible_size(self, row, trade_number):
        if row['asset_type'] != fx_spot or \
                abs(row['quantity']) > 0.02 * self.trade_size_map[trade_number]:
            return trade_number

    def find_matching_trade(self, row):
        if row['symbol'] in self.open_symbol_map:
            trade_number_map = self.open_symbol_map[row['symbol']]
            for trade_number, quantity in trade_number_map.items():
                if row['quantity'] * quantity < 0 and abs(row['quantity']) <= abs(quantity):
                    return self.if_plausible_size(row, trade_number)

    def merge_test_status(self, df):
        fund_index = df['asset_type'] == fund
        gbp_index = df['currency'] == 'GBP'
        small_open_index = (df['open_close'] == 'open') & (df['gross_transaction_value'] < self.TEST_SIZE)
        df.loc[:, 'trade_number'] = None
        test_index = (fund_index & ~gbp_index) | (fund_index & small_open_index)
        df.loc[test_index, 'trade_number'] = 'test'
        return df

    def merge_geography(self, df):
        name_maps = {index_future: {'Japan': ['TP', 'NK', 'TOPIX', 'Nikkei', 'Topix'],
                                    'US': ['ES', 'RTY', 'Russell', 'SP-MIN', 'SP-MINI', 'S&P']},
                    fund: {'Japan': ['Japan', 'Jpn', 'Fram', 'Jap'],
                           'US': ['US', 'Ame', 'American', 'Amer']},
                    fx_forward: {'Japan': ['JPY'],
                                 'US': ['USD']},
                    fx_spot: {'Japan': ['JPY'],
                              'US': ['USD'],
                              'other': ['EUR']}}

        def find_geo(geo_map, text):
            name_frags = text.replace('/', ' ').replace('(', ' ').replace(')', ' ').split(' ')
            for geo, words in geo_map.items():
                for word in words:
                    if word in name_frags:
                        return geo
            raise ValueError('cannot infer geography for ' + text)

        df.loc[:, 'geography'] = None
        for i, row in df.iterrows():
            if row['trade_number'] != 'test':
                asset_type = row['asset_type']
                if asset_type in name_maps:
                    geo_map = name_maps[asset_type]
                    geo = find_geo(geo_map, row['asset_name'])
                else:
                    geo = None
                df.at[i, 'geography'] = geo
        return df

    def merge_fund_trade_numbers(self, df):

        def find_equal_and_opposite_groups(df):
            x = df.groupby('trade_number').sum()['quantity'].reset_index()
            quantity_map = dict()
            regrouping_map = dict()
            for i, row in x.iterrows():
                trade_number = row['trade_number']
                quantity = row['quantity']
                if -quantity in quantity_map:
                    regrouping_map[trade_number] = quantity_map[-quantity]
                    del quantity_map[-quantity]
                elif quantity != 0:
                    quantity_map[quantity] = trade_number
            return regrouping_map

        trade_number = 0
        df = df.sort_values(['date', 'asset_type', 'open_close'])
        for i, row in df.iterrows():
            if (row['asset_type'] == fund) and (row['trade_number'] != 'test'):
                this_trade_number = None
                date_geo_key = (row['date'], row['geography'])
                if row['open_close'] == 'open':
                    if date_geo_key not in self.fund_open_date_map:
                        trade_number += 1
                        self.fund_open_date_map[date_geo_key] = trade_number
                    this_trade_number = self.fund_open_date_map.get(date_geo_key, 'error')
                    self.fund_symbol_map[row['symbol']] = this_trade_number
                    self.trade_size_map[this_trade_number] += row['gross_transaction_value']

                elif row['open_close'] == 'close':
                    this_trade_number = self.fund_symbol_map.get(row['symbol'], 'error')
                    self.fund_close_date_map[date_geo_key] = this_trade_number

                df.loc[i, 'trade_number'] = this_trade_number

        regrouping_map = find_equal_and_opposite_groups(df)
        df.loc[:, 'trade_number'] = df['trade_number'].replace(regrouping_map)

        for i, row in df.iterrows():
            if row['asset_type'] == fund and row['open_close'] == 'close':
                this_trade_number = row['trade_number']
                date_geo_key = (row['date'], row['geography'])
                self.fund_close_date_map[date_geo_key] = this_trade_number

        return df

    def merge_non_spot_open_close(self, df):
        df.loc[:, 'open_close'] = None
        df.loc[(df['asset_type'].isin([fund, fx_forward])) & (df['transaction_type'] == buy), 'open_close'] = 'open'
        df.loc[(df['asset_type'].isin([fund, fx_forward])) & (df['transaction_type'] == sell), 'open_close'] = 'close'
        df.loc[(df['asset_type'].isin([index_future])) & (df['transaction_type'] == sell), 'open_close'] = 'open'
        df.loc[(df['asset_type'].isin([index_future])) & (df['transaction_type'] == buy), 'open_close'] = 'close'
        return df

    def merge_spot_open_close(self, df):
        from numpy import sign
        df['cum_qty'] = df.groupby('symbol')['quantity'].transform(Series.cumsum)
        df.loc[:, 'prev_pos'] = df['cum_qty'] - df['quantity']
        fx_spot_index = df['asset_type'].isin([fx_spot])
        closing_index = (sign(df['prev_pos']) != sign(df['quantity'])) & (
                abs(df['quantity']) < 1.2 * abs(df['prev_pos']))
        spot_close_index = fx_spot_index & closing_index
        df.loc[fx_spot_index, 'open_close'] = 'conversion'
        df.loc[spot_close_index, 'open_close'] = 'close'
        return df

    def merge_non_fund_trade_numbers(self, df):
        df = df[df['trade_number'] != 'test']
        df = df[~df['geography'].isnull()]
        # df.loc[:, 'trade_number'] = to_numeric(df['trade_number'])
        df.loc[df['open_close'] == 'open', 'trade_number'] = df.groupby(['date', 'geography', 'open_close'])[
            'trade_number'].transform(Series.ffill)

        for i, row in df[df['asset_type'].isin([index_future, fx_spot, fx_forward])].iterrows():
            if row['open_close'] == 'open':
                trade_number = self.guess_open_trade_number(row)
                if trade_number:
                    df.at[i, 'trade_number'] = trade_number
                    self.update_open_map(row, trade_number)

            elif row['open_close'] == 'close':
                trade_number = self.find_matching_trade(row)
                if trade_number is None:
                    trade_number = self.guess_close_trade_number(row)
                if trade_number:
                    df.at[i, 'trade_number'] = trade_number
                    self.update_open_map(row, trade_number)
                    self.update_close_map(row, trade_number)

        return df

    def remove_test_trades(self, df):
        df = df[~df['unique'].isin(self.TEST_UNIQUES)]
        return df

    def merge_trade_sizes(self, df):
        self.trade_size_map['error'] = 0
        df.loc[:, 'trade_size_gbp'] = df['trade_number'].replace(self.trade_size_map)
        df.loc[:, 'trade_size_gbp'] = to_numeric(df['trade_size_gbp'])
        return df

    def merge_trade_classifications(self, df):
        df.loc[:, 'date'] = df['transaction_time'].dt.date
        df = self.merge_non_spot_open_close(df)
        df = self.merge_test_status(df)
        df = self.merge_geography(df)
        df = self.remove_test_trades(df)
        df = self.merge_fund_trade_numbers(df)
        df = self.merge_spot_open_close(df)
        df = self.merge_non_fund_trade_numbers(df)
        df = self.merge_trade_sizes(df)
        return df
