from pandas import DataFrame, to_datetime, to_timedelta, concat, to_numeric
from app.enums import cash, fx_forward, index_future, fx_spot, fund, reyl, transfer, sell, buy, interest, fee, dividend, mid_pacific_am, mft

from data.parse import FileParser


class ReylTransactionParser(FileParser):
    COLUMN_MAPPING = {'Description': 'asset_name',
                      'Value Date': 'value_date',
                      'ISIN / Account Nr.': 'symbol',
                      'Currency': 'currency',
                      'Instrument type': 'asset_type',
                      'Amount': 'quantity',
                      'Price': 'price',
                      'Brokerage': 'direct_fee',
                      'Broker commission': 'indirect_fee',
                      'Account amount': 'net_transaction_value',
                      'Booking Text': 'description',
                      'Transaction Type': 'transaction_type'}

    UNUSED_COLUMNS = ['Main Account Holder Name', 'Mandate Number', 'Mandate Name', 'Portfolio ', 'Portfolio Currency',
     'Portfolio Type', 'Transaction Reference', 'Accrued Interest', 'Account Nr. / ISIN',
     'Account Currency', 'Account Description', 'Total', 'Exchange rate', 'Cancellation']

    HEADER_ROW = 5

    MODEL_CLASS = 'Transaction'

    SOURCE = reyl

    @classmethod
    def parse_fx_forwards(cls, df: DataFrame):
        tx = df[df['transaction_type'].isin(['Fx forward (Sell)', 'Fx forward (Buy)']) & (
                df['asset_type'] == 'Forward Exchange')].copy()
        if len(tx.index) > 0:
            tx.loc[:, 'direct_commission'] = 0.0001
            tx.loc[:, 'indirect_commission'] = 0.0001
            tx.loc[:, 'asset_name'] = tx['asset_name'].str.replace('Your sale ', '').str.replace('Your purchase ', '')
            asset_name_split = tx['asset_name'].str.split(' ')
            reyl_symbols = asset_name_split.str[0]
            tx.loc[:, 'unique'] = tx['symbol']
            tx.loc[:, 'symbol'] = cls.convert_symbols(fx_forward, reyl_symbols)
            tx.loc[:, 'net_price'] = to_numeric(asset_name_split.str[1])
            tx.loc[:, 'asset_name'] = asset_name_split.str[0] + ' ' + asset_name_split.str[-1]
            tx.loc[:, 'transaction_type'] = tx['transaction_type'].str.replace('Fx forward \(', '').str.replace('\)', '').str.lower()
            tx.loc[:, 'asset_type'] = fx_forward
            tx.loc[tx['transaction_type'] == 'buy', 'price'] = tx['net_price'] / (1 + tx['direct_commission'] + tx['indirect_commission'])
            tx.loc[tx['transaction_type'] == 'sell', 'price'] = tx['net_price'] / (1 - tx['direct_commission'] - tx['indirect_commission'])
            tx.loc[:, 'gross_transaction_value'] = tx['price'] * tx['quantity']
            tx.loc[:, 'direct_fee'] = -abs(tx['gross_transaction_value'] * tx['direct_commission'])
            tx.loc[:, 'indirect_fee'] = -abs(tx['gross_transaction_value'] * tx['indirect_commission'])
            tx = tx.drop(['net_price', 'direct_commission', 'indirect_commission'], axis=1)
            tx = tx[tx['currency'] == tx['symbol'].str[0:3]]
            tx.loc[:, 'currency'] = tx['symbol'].str[3:6]
            tx.loc[:, 'net_transaction_value'] = -tx.loc[:, 'net_transaction_value']
            #tx = tx.drop_duplicates()
            cls.check_uniques(tx)
        return tx

    @classmethod
    def parse_fx_spots(cls, df: DataFrame):
        tx = df[df['transaction_type'].str.contains('Fx spot') &
                (df['description'].str.contains('Your sale') | df['description'].str.contains('Your purchase'))]
        if len(tx.index) > 0:
            tx.loc[:, 'direct_commission'] = 0.0001
            tx.loc[:, 'indirect_commission'] = 0.0001
            tx.loc[:, 'asset_name'] = tx['description'].str.replace('Your sale ', '').str.replace('Your purchase ', '')
            asset_name_split = tx['asset_name'].str.split(' ')
            reyl_symbols = asset_name_split.str[0]
            tx.loc[:, 'symbol'] = cls.convert_symbols(fx_spot, reyl_symbols)
            tx.loc[:, 'net_price'] = to_numeric(asset_name_split.str[1])
            tx.loc[:, 'asset_name'] = asset_name_split.str[0] + ' spot'
            tx.loc[tx['description'].str.contains('sale'), 'transaction_type'] = sell
            tx.loc[tx['description'].str.contains('purchase'), 'transaction_type'] = buy
            tx.loc[:, 'asset_type'] = fx_spot
            tx.loc[tx['transaction_type'] == 'buy', 'price'] = tx['net_price'] / (1 + tx['direct_commission'] + tx['indirect_commission'])
            tx.loc[tx['transaction_type'] == 'sell', 'price'] = tx['net_price'] / (1 - tx['direct_commission'] - tx['indirect_commission'])
            tx.loc[:, 'gross_transaction_value'] = tx['price'] * tx['quantity']
            tx.loc[:, 'direct_fee'] = -abs(tx['gross_transaction_value'] * tx['direct_commission'])
            tx.loc[:, 'indirect_fee'] = -abs(tx['gross_transaction_value'] * tx['indirect_commission'])
            tx = tx.drop(['net_price', 'direct_commission', 'indirect_commission'], axis=1)
            tx.loc[:, 'currency'] = tx['symbol'].str[3:6]
            tx.loc[:, 'net_transaction_value'] = -tx.loc[:, 'net_transaction_value']
        return tx

    @classmethod
    def parse_futures(cls, df: DataFrame):
        tx = df[(df['asset_type'] == 'Options/Warrants') & df['transaction_type'].isin(['Buy', 'Sell'])].copy()
        if len(tx.index) > 0:
            tx.loc[:, 'transaction_type'] = tx['transaction_type'].str.lower()
            tx.loc[:, 'asset_type'] = index_future
            tx.loc[:, 'unique'] = tx['description'].str.split(' / ').str[-1]
            tx.loc[:, 'symbol'] = tx['asset_name'].str.split('\(').str[1]
            tx.loc[:, 'symbol'] = tx['symbol'].str.replace(',', '')
            tx.loc[:, 'symbol'] = tx['symbol'].str.split('\)').str[0]
            tx.loc[tx['symbol'].isnull(), 'symbol'] = tx['asset_name']
            tx.loc[:, 'symbol'] = cls.convert_symbols(index_future, tx['symbol'])
            tx.loc[:, 'price'] = None
            tx.loc[:, 'gross_transaction_value'] = None
            tx.loc[:, 'net_transaction_value'] = None
            cls.check_uniques(tx)
        return tx

    @classmethod
    def parse_funds(cls, df):
        tx = df[(df['asset_type'] == 'Funds') & (df['transaction_type'].isin(['Buy', 'Sell'])) & ~df['description'].str.contains('AUTOM.GENERATED')].copy()
        if len(tx.index) > 0:
            tx.loc[:, 'transaction_type'] = df['transaction_type'].str.lower()
            tx.loc[:, 'symbol'] = cls.convert_symbols(fund, tx['symbol'])
            tx.loc[:, 'asset_type'] = fund
            tx.loc[:, 'gross_transaction_value'] = tx['price'] * tx['quantity']
            tx.loc[:, 'unique'] = tx['description'].str.split(' / ').str[-1]
            if 'SE Swiss Stamp' in tx.columns:
                tx.loc[:, 'tax'] = tx['SE Swiss Stamp']
            else:
                tx.loc[:, 'tax'] = 0
            tx.loc[:, 'net_transaction_value'] = tx['gross_transaction_value'] + tx['direct_fee'] + tx['indirect_fee'] + tx['tax']
        return tx

    @classmethod
    def parse_admin_fees(cls, df):
        commercial_gesture_index = df['description'].str.contains('Commercial gesture')
        custody_fee_index = df['description'].str.contains('Custody fee')
        admin_fee_index = df['description'].str.contains('Administration Fee')
        tx = df[commercial_gesture_index | custody_fee_index | admin_fee_index].copy()
        if 'Custody Fees' in tx.columns:
            tx.loc[:, 'quantity'] += -to_numeric(tx['Custody Fees']).fillna(0)
        if 'Administration Fees' in tx.columns:
            tx.loc[:, 'quantity'] += -tx['Administration Fees']
        if len(tx.index) > 0:
            tx.loc[:, 'asset_name'] = tx['currency']
            tx.loc[:, 'symbol'] = tx['currency']
            tx.loc[:, 'asset_type'] = cash
            tx.loc[:, 'transaction_type'] = fee
            tx.loc[:, 'gross_transaction_value'] = tx['quantity']
        return tx

    @classmethod
    def parse_interest(cls, df):
        tx = df[(df['transaction_type'] == 'Debit interest') & (df['asset_type'] == 'Liquidities')].copy()
        if len(tx.index) > 0:
            tx.loc[:, 'asset_type'] = cash
            tx.loc[:, 'transaction_type'] = interest
            tx.loc[:, 'asset_name'] = tx['asset_name'].str.split(' ').str[-1]
            tx.loc[:, 'symbol'] = tx['asset_name']
            tx.loc[:, 'gross_transaction_value'] = tx['quantity']
        return tx

    @classmethod
    def parse_dividends(cls, df):
        tx = df[(df['transaction_type'] == 'Dividends/Coupons/Earnings(Asset-linked)') & (df['asset_type'] == 'Funds')].copy()
        if len(tx.index) > 0:
            tx.loc[:, 'gross_transaction_value'] = tx['net_transaction_value']
            tx.loc[:, 'direct_fee'] = 0
            tx.loc[:, 'indirect_fee'] = 0
            tx.loc[:, 'asset_type'] = cash
            tx.loc[:, 'transaction_type'] = dividend
            tx.loc[:, 'asset_name'] = tx['symbol'] + ' (Dividend)'
            tx['symbol'] = tx['currency']
        return tx

    @classmethod
    def parse_transfers(cls, df):
        tx = df[df['transaction_type'].isin(['Cash inflow', 'Cash outflow'])
                & (df['asset_type'] == 'Liquidities')
                & (df['description'].str.contains('NO. 5 1 9 8 9 0')
                   | df['description'].str.contains('SPENCER-CHURCHILL')
                   | df['description'].str.contains('TRANSFER'))].copy()
        if len(tx.index) > 0:
            tx.loc[:, 'asset_type'] = cash
            tx.loc[:, 'transaction_type'] = transfer
            tx.loc[:, 'asset_name'] = tx['currency']
            tx.loc[:, 'symbol'] = tx['currency']
            tx.loc[:, 'gross_transaction_value'] = tx['quantity']
        return tx

    def parse(self):
        df = ReylTransactionParser.prepare_dataframe(self.file_contents, self.HEADER_ROW,
                                                     self.UNUSED_COLUMNS,
                                                     self.COLUMN_MAPPING)
        hm_time_index = df['Trade Time'].str.contains('^[0-9]*:[0-9]*$')
        df.loc[hm_time_index, 'Trade Time'] = df['Trade Time'] + ':00'
        df.loc[:, 'transaction_time'] = to_datetime(df['Transaction Date']) + to_timedelta(df['Trade Time'])
        df = df[~df['transaction_type'].isnull()]
        df.loc[:, 'unique'] = None

        fx_forward_txs = self.parse_fx_forwards(df)
        fx_spot_txs = self.parse_fx_spots(df)
        fut_txs = self.parse_futures(df)
        fund_txs = self.parse_funds(df)
        interest_txs = self.parse_interest(df)
        dividend_txs = self.parse_dividends(df)
        fee_txs = self.parse_admin_fees(df)
        transfer_txs = self.parse_transfers(df)
        df = concat([fx_forward_txs, fx_spot_txs, fut_txs, fund_txs, interest_txs, dividend_txs, fee_txs, transfer_txs], sort=False)

        df = self.set_timezone(df, ['value_date', 'transaction_time'], 'utc')
        for col in ['gross_transaction_value', 'price']:
            df.loc[~df['gross_transaction_value'].isnull(), col] = to_numeric(df[col]).fillna(0)
        for col in ['direct_fee', 'indirect_fee', 'tax']:
            if col not in df.columns:
                df.loc[:, col] = 0
            else:
                df.loc[:, col] = -abs(to_numeric(df[col], errors="coerce")).fillna(0)
        df.loc[df['net_transaction_value'].isnull() & ~df['gross_transaction_value'].isnull(), 'net_transaction_value'] = df['gross_transaction_value'] - df['direct_fee'] - df['indirect_fee'] - df['tax']

        df.loc[:, 'custodian'] = reyl
        df.loc[:, 'owner'] = mid_pacific_am
        df.loc[:, 'group'] = mft

        df = df.sort_values(by='transaction_time')
        return {'Transaction': df}
