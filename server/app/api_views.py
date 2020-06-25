import hashlib
from datetime import datetime

from utils import api_response, percentage_formatter, float_formatter, datetime_formatter, Message
from data.parse import FileParser
from data.parse.json_model import JSONParser
from django.http import JsonResponse
from app.asset import Asset
from data.http import merge_request_fields
from data.http.ft import parse_daily
from app.enums import reyl, bloomberg
from app.enums import index_future as future_asset_type
from reporting.reconciliation import end_of_day, cash_rec_summary
from trading.mft import signal_data, contract_information
from app.models import CashMovement, Transaction, to_dataframe
from rest_framework.decorators import api_view


@api_view()
@api_response()
def aviva_funds(request):
    funds = ["FR0007017488"]
    return JsonResponse(funds)


@api_view()
@api_response()
def recent_fund_prices(request, isin, currency):
    prices = parse_daily(isin, currency, "fund")
    data = merge_request_fields(isin, "daily", "ft.com", "fund", prices)
    return data


@api_view()
@api_response(structure=["index", "future", "future price", "index price", "fair spread",
                         "gross signal", "net signal", "time"],
              formatters={'future price': float_formatter,
                          'index price': float_formatter,
                          'fair spread': float_formatter,
                          'gross signal': percentage_formatter,
                          'net signal': percentage_formatter,
                          'time': datetime_formatter})
def signals(request):
    def hash(row):
        m = hashlib.md5()
        m.update(row['index'].encode('utf-8'))
        m.update(str(row['time']).encode('utf-8'))
        return m.hexdigest()
    global signals_formatters
    data = signal_data()
    data = data.rename(columns={'gross_signal': 'gross signal', 'net_signal': 'net signal'})
    data.loc[:, 'hash'] = data.apply(hash, axis=1)
    return data


@api_view()
@api_response(structure=CashMovement)
def reyl_cash_movements(request):
    models = CashMovement.objects.filter(custodian=reyl).order_by('transaction_date')
    data = to_dataframe(models, 'classification')
    return data


@api_view()
@api_response(structure=Transaction)
def reyl_transactions(request):
    models = Transaction.objects.filter(custodian=reyl).order_by('transaction_time')
    data = to_dataframe(models)
    return data


@api_view(['GET', 'POST'])
@api_response(structure=Transaction)
def reyl_transactions_with_no_price(request):
    if request.method == 'GET':
        models = Transaction.objects.filter(custodian=reyl,
                                            asset_type=future_asset_type,
                                            price__isnull=True)
        data = to_dataframe(models)
        return data
    elif request.method == 'POST':
        try:
            prices = request.POST['prices']
            import json
            data = json.loads(prices)
            for id, price in data.items():
                tx = Transaction.objects.get(id=id, asset_type=future_asset_type)
                tx.price = float(price)
                future = Asset.create_asset(tx.symbol, future_asset_type, bloomberg)
                mult = future.contract_mult()
                tx.gross_transaction_value = tx.price * tx.quantity * mult
                tx.net_transaction_value = tx.gross_transaction_value + tx.direct_fee + tx.indirect_fee
                tx.save()
            return Message('Updated the futures transactions', 'success').as_response()
        except Exception as e:
            return Message(str(e), 'error')


@api_view()
@api_response(structure=['name', 'country', 'exchange', 'id', 'currency', 'expiration', 'contract_size_gbp', 'as_of'],
              formatters={'expiration': datetime_formatter,
                          'as_of': datetime_formatter,
                          'contract_size_gbp': float_formatter})
def contract_info(request):
    return contract_information()


@api_view(['GET', 'POST'])
@api_response()
def handle_file_upload(request):
    if request.method == 'POST':
        message = ''
        oks = 0
        for name, file in request.FILES.items():
            _, text = FileParser.parse_and_save(file, save_new=True)
            if text == '':
                oks += 1
            else:
                message += name + ': ' + text + ', '
        if oks == len(request.FILES):
            return Message('All files processed', 'success')
        else:
            return Message('Some files failed | ' + message, 'error')


@api_view(['POST'])
@api_response()
def handle_json_model(request):
    if request.method == 'POST':
        json = request.POST['data']
        parser = JSONParser.create(json)
        data = parser.parse()
        _, text = parser.save(data, save_new=True, update_existing=False)
        if text == '':
            return Message('POST request processed', 'success')
        else:
            return Message('POST request failed | ' + text, 'error')


@api_view()
@api_response()
def reyl_cash_movement_summary(request):
    date_str = request.GET.get('to', None)
    if date_str is None:
        date = datetime.today()
    else:
        date = datetime.strptime(date_str, '%Y-%m-%d')
    group_by = request.GET.get('group_by', None)
    if group_by is not None:
        group_by = group_by.split(',')
    errors_only = bool(request.GET.get('errors_only', False))
    time = end_of_day(date)
    summary = cash_rec_summary(time, group_by=group_by, errors_only=errors_only, custodian=reyl)
    return summary


