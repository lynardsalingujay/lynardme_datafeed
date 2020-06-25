from datetime import datetime
from collections import OrderedDict

from pandas import Series, merge, to_numeric, concat, to_datetime, DataFrame
from app.enums import buy, sell, fund, fx_forward, index_future, fx_spot, reyl, cash, interest, exante
from reporting.matcher import TradeMatcher


def merge_fx_rates(df):
    fx = df.copy()
    fx = fx[(fx['open_close'] == 'close') & (fx['asset_type'].isin([fx_forward, fx_spot]))]
    fx = fx.groupby(['trade_number', 'currency']).sum()
    fx.loc[:, 'fx_rate'] = fx['gross_transaction_value'] / fx['quantity']
    fx = fx[['fx_rate']].reset_index()
    df = merge(df, fx, how='left', on=['trade_number', 'currency'])
    df.loc[df['currency'] == 'GBP', 'fx_rate'] = 1
    return df


def merge_trade_sizes(self, df):
    ts = df.copy()
    ts = ts[(ts['open_close'] == 'open') & (ts['asset_type'] == fund)]
    ts = ts.groupby(['trade_number']).sum()
    ts.loc[:, 'trade_size_gbp'] = ts['gross_transaction_value']
    ts = ts[['trade_size_gbp']].reset_index()
    df = merge(df, ts, how='left', on=['trade_number'])
    return df


def merge_start_and_end_dates(df):
    start, end = df.copy(), df.copy()
    start = start[(start['open_close'] == 'open') & (start['asset_type'] == fund)]
    end = end[(end['open_close'] == 'close') & (end['asset_type'] == fund)]
    start = start.groupby(['trade_number']).min().reset_index()
    end = end.groupby(['trade_number']).max().reset_index()
    start.loc[:, 'start_date'] = to_datetime(start['date'])
    end.loc[:, 'end_date'] = to_datetime(end['date'])
    df = merge(df, start[['start_date', 'trade_number']], how='left', on=['trade_number'])
    df = merge(df, end[['end_date', 'trade_number']], how='left', on=['trade_number'])
    return df


def merge_total(df):
    agg_map = {'start_date': 'min',
               'end_date': 'max',
               'trade_size_gbp': 'sum',
               'gross_pnl_gbp': 'sum',
               'fees_gbp': 'sum',
               'net_pnl_gbp': 'sum'}
    total = df.agg(agg_map).to_frame().transpose()
    total.loc[:, 'net_return'] = total['net_pnl_gbp'] / total['trade_size_gbp']
    total.loc[:, 'trade_number'] = 'TOTAL'
    total.loc[:, 'geography'] = ''
    df = concat([df, total])
    return df


def merge_interest(df, totals):

    def interest_txs(summary, quantity_col, currency_col):
        df = summary.copy()
        df.loc[:, 'asset_type'] = cash
        df.loc[:, 'transaction_type'] = interest
        df.loc[:, 'direct_fee'] = -abs(summary[quantity_col])
        df.loc[:, 'currency'] = summary[currency_col]
        df['indirect_fee'] = 0
        df['tax'] = 0
        df['gross_transaction_value'] = 0
        return df

    currency_map = {'JPY': 'Japan',
                    'USD': 'US',
                    'EUR': '',
                    'GBP': ''}
    reverse_currency_map = dict()
    for cur, geo in currency_map.items():
        reverse_currency_map[geo] = cur

    totals.loc[:, 'geography'] = totals['currency'].replace(currency_map)
    totals = totals.groupby('currency').agg({'quantity': 'sum', 'geography': 'max'})
    df.loc[:, 'duration'] = df['end_date'] - df['start_date']

    agg_map = {'trade_size_gbp': 'min',
               'duration': 'min', 'geography': 'min',
               'start_date': 'min',
               'end_date': 'min'}
    summary = df.groupby('trade_number').agg(agg_map)
    summary.loc[:, 'factor'] = summary['trade_size_gbp'] * to_numeric(summary['duration'])
    summary.loc[:, 'gbp_prop'] = summary['factor'] / summary['factor'].sum()
    for geo in set(summary['geography'].values):
        index = summary['geography'] == geo
        summary.loc[index, 'non_gbp_prop'] = summary.loc[index, 'factor'] / summary.loc[index, 'factor'].sum()
    summary.loc[:, 'trade_number'] = summary.index
    summary = merge(summary, totals, how='left', on='geography')
    summary.loc[:, 'gbp_quantity'] = totals.loc['GBP', 'quantity']
    summary.loc[:, 'gbp_allocation'] = summary['gbp_quantity'] * summary['gbp_prop']
    summary.loc[:, 'non_gbp_allocation'] = summary['quantity'] * summary['non_gbp_prop']
    summary.loc[:, 'non_gbp_currency'] = summary['geography'].replace(reverse_currency_map)
    summary.loc[:, 'gbp_currency'] = 'GBP'

    gbp_lines = interest_txs(summary, 'gbp_allocation', 'gbp_currency')
    non_gbp_lines = interest_txs(summary, 'non_gbp_allocation', 'non_gbp_currency')

    return concat([df, gbp_lines, non_gbp_lines]).reset_index()


def merge_classification(df):
    index = df['asset_type'] == cash
    df.loc[index, 'classification'] = df['transaction_type']
    df.loc[~index, 'classification'] = df['asset_type']
    return df


def trade_summary(group_by=None, **criteria):
    from app.models import Transaction, to_dataframe
    txs = Transaction.objects.filter(custodian=reyl)
    df = to_dataframe(txs)
    interest = df[df['transaction_type'] == 'interest'].copy()
    matcher = TradeMatcher()
    df = matcher.merge_trade_classifications(df)
    df = merge_start_and_end_dates(df)
    df = merge_interest(df, interest)
    df = merge_classification(df)
    df = merge_fx_rates(df)
    df.loc[:, 'fees'] = df['direct_fee'] + df['indirect_fee'] + df['tax']
    fx_index = df['asset_type'].isin([fx_forward, fx_spot])
    df.loc[fx_index, 'value_at_end'] = df['quantity'] * df['fx_rate']
    df.loc[~fx_index, 'value_at_end'] = 0
    df.loc[:, 'gross_pnl'] = df['value_at_end'] - df['gross_transaction_value']
    df.loc[:, 'net_pnl'] = df['gross_pnl'] + df['fees']
    df.loc[:, 'net_pnl_gbp'] = df['net_pnl'] / df['fx_rate']
    df.loc[:, 'gross_pnl_gbp'] = df['gross_pnl'] / df['fx_rate']
    df.loc[:, 'fees_gbp'] = df['fees'] / df['fx_rate']
    agg_map = {'start_date': 'min',
               'end_date': 'max',
               'geography': 'max',
               'gross_pnl_gbp': 'sum',
               'fees_gbp': 'sum',
               'net_pnl': 'sum',
               'fx_rate': 'mean',
               'net_pnl_gbp': 'sum',
               'quantity': 'sum',
               'value_at_end': 'sum',
               'trade_size_gbp': 'mean'}
    grouped = df.groupby(group_by).agg(agg_map).reset_index()
    grouped.loc[:, 'net_return'] = grouped['net_pnl_gbp'] / grouped['trade_size_gbp']
    #if 'currency' in group_by:
    cols = group_by + ['start_date', 'end_date', 'geography', 'trade_size_gbp', 'gross_pnl_gbp', 'fees_gbp', 'net_pnl_gbp', 'net_return']
    #else:
    #    cols = ['gross_pnl_gbp', 'costs_gbp', 'net_pnl_gbp']
    # 'value_at_end', 'gross_pnl', 'costs', 'net_pnl', 'fx_rate', 'quantity'
    grouped = merge_total(grouped)
    grouped = grouped[cols]
    return grouped


def find_opening_price(as_of, symbol, quantity):
    txs = Transaction.objects.filter(transaction_time__lt=as_of, symbol=symbol)
    txs = to_dataframe(txs)
    txs = txs.sort_values('transaction_time')
    cum_qty, row_indexes = 0, []
    for index, row in txs.iterrows():
        if cum_qty < quantity:
            cum_qty += quantity
            row_indexes.append(index)
    txs = txs[row_indexes]
    txs.loc[:, 'weighted_price'] = txs['price'] * txs['quantity']
    average_price = txs['weighted_price'].sum() / txs['quantity'].sum()
    return average_price


def merge_opening_prices(as_of, df):
    symbols = df[df['asset_type'].isin([future, fx_forward])]
    replace_map = dict()
    for symbol in symbols:
        replace_map[symbol] = find_opening_price(as_of)
    return df


def valuation(as_of: datetime, group_by=None, **criteria):
    from reporting.marks import get_all_marks
    df = to_dataframe(Position.objects.filter(as_of_date=as_of, **criteria))
    all_marks = get_all_marks(df, as_of)
    cols = []
    for (source, (prices, fx_rates)) in all_marks.items():
        df = df.merge(prices, how='left', on=['symbol', 'asset_type'])
        df = df.merge(fx_rates, how='left', on='currency')
        df = df.rename(columns={'price': source + '_price', 'fx_rate': source + '_fx_rate'})
        df.loc[:, source + '_gbp_value'] = df[source + '_price'] * df[source + '_fx_rate'] * df['quantity']
        cols.append(source + '_gbp_value')
    summary = df.sum().to_frame().transpose()
    for col in group_by:
        summary.loc[:, col] = 'total_value'
    if group_by is not None:
        grouped = df.groupby(group_by).sum().reset_index()
        summary = concat([grouped, summary])
    return summary.reset_index()[group_by + cols]


def risk_report(as_of, **criteria):
    group_by = ['asset_type']
    df = valuation(as_of, group_by, **criteria)
    df = df.set_index('asset_type').transpose()
    df.loc[:, 'ltv'] = -df['cash'] / df['total_value']
    df.loc[df['ltv'] < 0, 'ltv'] = 0
    return df.transpose()


def performance_report(as_of, group_by=None, **criteria):

    pass


def load_reyl_data(local_folders, error_files):
    from data.parse import FileParser
    from app.models import Transaction, CashMovement, to_dataframe
    from settings import ENVIRONMENT
    from utils.test import list_files, test_data_dir

    def list_all_files():
        for mask in ['.xlsx', '.pdf', '.json']:
            for dir in local_folders:
                for file_name in list_files(dir, mask_include=mask, mask_exclude='#'):
                    if file_name not in error_files:
                        yield file_name

    if ENVIRONMENT == 'DEV':
        Transaction.objects.all().delete()
        CashMovement.objects.all().delete()
        Position.objects.all().delete()
        Price.objects.all().delete()

        for file_name in list_all_files():
            FileParser.parse_and_save(file_name, save_new=True, update_existing=True)
    else:
        raise ValueError('only do this in DEV!')


if __name__ == '__main__':
    import wsgi
    from app.models import to_dataframe, Transaction, Position, Price
    from utils.test import list_files, test_data_dir
    from os.path import join

    action = 'risk'

    if action == 'load_data':
        error_files = []
        legacy = join(test_data_dir, 'reyl_legacy')
        #for file_name in error_files:
        #    FileParser.parse_and_save(file_name, save_new=True, update_existing=True)
        load_reyl_data(['/home/alex/Documents/MFT',
                        legacy
                        ], error_files)
        summary = to_dataframe(Transaction.objects.all())
        summary = summary[(summary['asset_type'] == future) & summary['price'].isnull()]
    elif action == 'parse_file':
        Position.objects.all().delete()
        Price.objects.all().delete()
        from data.parse import FileParser
        FileParser.parse_and_save('/home/alex/Documents/MFT/wip/Detailed_Positions (1).xlsx', save_new=True, update_existing=True)
        FileParser.parse_and_save('/home/alex/Documents/MFT/FX_Rates (1).xlsx', save_new=True, update_existing=True)
    elif action == 'trade_summary':
        summary = trade_summary(group_by=['trade_number'
        , 'classification'
                                      ])
    elif action == 'valuation':

        summary = valuation(datetime(2019, 11, 21), group_by=None, custodian=reyl)

    elif action == 'risk':
        summary = risk_report(datetime(2019, 12, 4), custodian=reyl)
    #elif action == 'reconciliation':
    #    cash_rec_summary
    print(summary)