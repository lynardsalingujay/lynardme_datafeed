from app.enums import interest, dividend, transfer, fee, fx_forward, fx_spot, fund, index_future, fx_future, cash

from pandas import concat, DataFrame
from collections import namedtuple
from datetime import datetime
from pytz import timezone

CashRec = namedtuple('CashReconciliation', 'classification currency cash_movement transaction position ok '
                                           'transaction_date value_date')

PositionRec = namedtuple('PositionReconciliation', 'classification asset transaction position ok')

columns = ['currency', 'amount']

group_by_columns = ['transaction_date', 'value_date', 'classification']


def to_tuples(df: DataFrame, tuple_cls):
    tuples = []
    for d in df.to_dict(orient='records'):
        tuples.append(tuple_cls(**d))
    return tuples


def cash_movement_summary(as_of: datetime, group_by, exclude_futures=False, **criteria):
    from app.models import CashMovement, to_dataframe
    cash_movements = CashMovement.objects.filter(**criteria).all()
    if len(cash_movements) > 0:
        df = to_dataframe(cash_movements, 'classification')
        df = df[df['transaction_date'] < as_of]
        df.loc[:, 'transaction_date'] = df['transaction_date'].dt.date
        df.loc[:, 'value_date'] = df['value_date'].dt.date
        df.loc[:, 'amount'] = df['credit_amount'] - df['debit_amount']
        if exclude_futures:
            df = df[df['classification'] != 'future']
        grouped = df.groupby(group_by).sum()[['amount']]
    else:
        grouped = DataFrame(columns=['amount'])
    return grouped.rename(columns={'amount': 'cash_movement'})


def classify_cash_txs(transaction_type, txs):
    index = txs['transaction_type'] == transaction_type
    txs.loc[index, 'classification'] = transaction_type
    txs.loc[index, 'amount'] = txs['net_transaction_value']
    return txs

  
def classify_future_txs(asset_type, txs, exclude_futures):
    index = txs['asset_type'] == asset_type
    txs.loc[index, 'classification'] = asset_type
    if exclude_futures:
        txs.loc[index, 'amount'] = txs['direct_fee'] + txs['indirect_fee']
    else:
        txs.loc[index, 'amount'] = -txs['net_transaction_value']
    return txs


def classify_non_cash_txs(asset_type, txs):
    index = txs['asset_type'] == asset_type
    txs.loc[index, 'classification'] = asset_type
    txs.loc[index, 'amount'] = -txs['net_transaction_value']
    return txs


def calculate_fx_counter_legs(df):
    fxs = []
    for asset_type in [fx_forward, fx_spot]:
        fx = df[df['asset_type'] == asset_type].copy()
        fx.loc[:, 'classification'] = asset_type
        fx.loc[:, 'amount'] = fx['quantity']
        fx.loc[:, 'currency'] = fx['symbol'].str[:3]
        fxs.append(fx)
    return fxs


def tx_cash_summary(as_of: datetime, group_by, exclude_futures=False, **criteria):
    from app.models import Transaction, to_dataframe
    txs = Transaction.objects.filter(**criteria).order_by('transaction_time')
    if len(txs) > 0:
        df = to_dataframe(txs)
        df = df.rename(columns={'transaction_time': 'transaction_date'})
        df = df[df['transaction_date'].dt.floor('D') <= as_of]
        df.loc[:, 'transaction_date'] = df['transaction_date'].dt.date
        df.loc[:, 'value_date'] = df['value_date'].dt.date
        if len(df.index) == 0:
            return DataFrame(columns=['amount'])
        for asset_type in [index_future, fx_future]:
            df = classify_future_txs(asset_type, df, exclude_futures)
        for asset_type in [fund, fx_spot, fx_forward]:
            df = classify_non_cash_txs(asset_type, df)
        for tx_type in [interest, dividend, transfer, fee]:
            df = classify_cash_txs(tx_type, df)
        fxs = calculate_fx_counter_legs(df)
        df = concat([df] + fxs)

        grouped = df.groupby(group_by).sum()[['amount']]
    else:
        grouped = DataFrame(columns=['amount'])
    return grouped.rename(columns={'amount': 'transaction'})


def merge_recs(cash_movements, txs, pos):
    dfs = []
    for df in [cash_movements, txs, pos]:
        if len(df.index) > 0:
            dfs.append(df)
    if len(dfs) > 0:
        summary = concat(dfs, axis=1).reset_index()
        for col in ['transaction', 'cash_movement', 'position']:
            if col not in summary:
                summary.loc[:, col] = 0
        for col in group_by_columns:
            if col not in summary:
                summary.loc[:, col] = '*'
        summary.loc[:, 'transaction'] = summary['transaction'].fillna(0)
        summary.loc[:, 'cash_movement'] = summary['cash_movement'].fillna(0)
        summary.loc[:, 'position'] = summary['position'].fillna(0)
    else:
        summary = DataFrame()
    return summary


def position_summary(as_of: datetime, group_by, exclude_futures=False, **criteria):
    from app.models import Position, to_dataframe
    pos = Position.objects.filter(**criteria).order_by('as_of_date')
    if len(pos) > 0:
        df = to_dataframe(pos)
        df = df[df['as_of_date'] == as_of]
        df = df[df['asset_type'] == cash]
        df = df.rename(columns={'quantity': 'amount'})
        df.loc[:, 'as_of_date'] = df['as_of_date'].dt.date
        if len(df.index) == 0:
            return DataFrame(columns=['amount'])
        if exclude_futures:
            df = df[df['asset_type'] != 'future']
        grouped = df.groupby(group_by).sum()[['amount']]
    else:
        grouped = DataFrame(columns=['amount'])
    return grouped.rename(columns={'amount': 'position'})


def cash_rec_summary(as_of: datetime, group_by=None, errors_only=False, exclude_futures=False, **criteria):
    if group_by is None:
        group_by = []
    if 'currency' not in group_by:
        group_by.append('currency')

    cash_movements = cash_movement_summary(as_of, group_by, exclude_futures=exclude_futures, **criteria)
    txs = tx_cash_summary(as_of, group_by, exclude_futures=exclude_futures, **criteria)
    pos = position_summary(as_of, group_by, exclude_futures=exclude_futures, **criteria)

    summary = merge_recs(cash_movements, txs, pos)
    if len(summary.index) == 0:
        return DataFrame(columns=[])
    if exclude_futures:
        summary = summary[~(summary['classification'] == index_future)]

    if cash_movements.shape[0] == 0:
        summary.loc[:, 'ok'] = abs(summary['position'] - summary['transaction']) < 1
    elif txs.shape[0] == 0:
        summary.loc[:, 'ok'] = abs(summary['cash_movement'] - summary['position']) < 1
    elif pos.shape[0] == 0:
        summary.loc[:, 'ok'] = abs(summary['cash_movement'] - summary['transaction']) < 1
    else:
        raise NotImplemented("Has not implemented!")

    if len(summary.index > 0):
        summary.loc[:, 'position'] = None
    if errors_only:
        summary = summary[~summary['ok']]
    return summary


def end_of_day(date=None):
    if date is None:
        date = datetime.today()
    return datetime(date.year, date.month, date.day, 23, 59, 59, tzinfo=timezone('UTC'))


if __name__ == '__main__':
    import wsgi
    from pandas import to_datetime
    as_of_date = to_datetime(datetime(2019, 12, 1), utc=True)
    cash_summary = cash_rec_summary(as_of_date, group_by=['value_date', 'classification'], exclude_futures=True)
    print(cash_summary)