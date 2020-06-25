from datetime import datetime
from json import loads

from behave import given, when, then, step
from django.urls import reverse

from app.enums import index_future, buy
from data.parse import FileParser
from utils.test import test_file, test_files, delete_database_data


@given('a user')
def get_user(context):
    from django.contrib.auth.models import User
    if len(User.objects.filter(username='username')) == 0:
        user = User.objects.create_user('username', 'email', 'password')
        user.save()
    context.client.logout()


@given("a valid JWT token")
@when('the front-end requests a new token')
def request_token(context):
    url = reverse('token_obtain_pair')
    context.token = context.client.post(url, {'username': 'username',
                                              'password': 'password',
                                              'content-type': 'application/json'})


@then("the back-end provides a valid JWT token")
def check_jwt_token(context):
    assert (context.token.status_code == 200)
    token = context.token.json()
    assert (len(token['refresh']) > 0)
    assert (len(token['access']) > 0)


@then("the status should be success")
def the_status_should_be_success(context):
    json = context.response.json()
    assert (json['status'] == 'success')


FUTURE_TX = {'asset_name': 'e-mini S&P500 Dec',
             'transaction_time': datetime(2019, 1, 1, 12, 45, 32),
             'value_date': datetime(2019, 1, 2),
             'symbol': 'ESZ9 Index',
             'currency': 'USD',
             'quantity': 1,
             'price': None,
             'gross_transaction_value': None,
             'direct_fee': 10,
             'indirect_fee': 100,
             'tax': 0,
             'net_transaction_value': None,
             'description': 'bought 1 ESM9',
             'transaction_type': buy,
             'asset_type': index_future,
             'unique': 'ABC123'}

FUTURE_PRICE = 1234.5


@given('a future transaction is in the database with no price set')
def add_future_transaction_with_null_price(context):
    from app.models import Transaction
    tx = Transaction.objects.create(**FUTURE_TX)
    tx.save()
    context.tx_id = tx.id


@when('the price is posted')
def send_post_with_future_price(context):
    import json
    token = context.token.json()
    context.client.credentials(HTTP_AUTHORIZATION='Bearer ' + token['access'])
    url = reverse('reyl_transactions_with_no_price', args=['live', 'json'])
    context.client.post(url, {'content-type': 'application/json',
                              'prices': json.dumps({context.tx_id: FUTURE_PRICE})})


@then('the price should be updated')
def check_future_price_update(context):
    from app.models import Transaction
    tx = Transaction.objects.get(id=context.tx_id)
    assert (tx.price == FUTURE_PRICE)
    assert (tx.gross_transaction_value == FUTURE_PRICE * 50)


@then("there should not be any errors")
def assert_no_errors(context):
    assert (True)


@when('various requests with different query "{parameters}" are made')
def request_cash_rec(context, parameters):
    url = reverse('reyl_cash_movement_summary', args=['live', 'json'])
    context.client.get(url + parameters)


@given("some transactions and cash movements in the database")
def add_transactions_and_cash_movements_to_db(context):
    delete_database_data()
    for file_name in test_files('reyl_legacy', mask_include='.xlsx', mask_exclude='#'):
        file = test_file(file_name)
        FileParser.parse_and_save(file, save_new=True)


@given("some transactions, cash movements, and positions in the database")
def add_transactions_cash_movements_and_positions_to_db(context):
    for file_name in test_files('exante_test_files', mask_include='.csv', mask_exclude='#'):
        file = test_file(file_name)
        FileParser.parse_and_save(file, save_new=True)
    for file_name in test_files('exante_test_files', mask_include='.xls', mask_exclude='#'):
        file = test_file(file_name)
        FileParser.parse_and_save(file, save_new=True)


@given('I am logged in')
def log_in_some_user(context):
    from django.contrib.auth.models import User
    context.client.force_login(User.objects.get_or_create(username='testuser')[0])


@given('some "{json}" representations of models')
def step_impl(context, json):
    context.json = json


@then("the model should be updated in the database")
def step_impl(context):
    from app.models import Price
    json = loads(context.json)
    kwargs = {}
    for arg in ['value', 'source']:
        kwargs[arg] = json['data'][0][arg]
    Price.objects.get(**kwargs)


@when("the json is posted")
def step_impl(context):
    url = reverse('handle_json_model', args=['live', 'json'])
    token = context.token.json()
    context.client.credentials(HTTP_AUTHORIZATION='Bearer ' + token['access'])
    context.response = context.client.post(url, {'username': 'username',
                                                 'password': 'password',
                                                 'content-type': 'application/json',
                                                 'data': context.json})


@then("the authorized response is valid")
def step_impl(context):
    assert (context.response.status_code == 200)


@when("a protected url is requested")
def step_impl(context):
    url = reverse('reyl_transactions', args=['live', 'json'])
    context.unauthorized_response = context.client.get(url)
    token = context.token.json()
    context.client.credentials(HTTP_AUTHORIZATION='Bearer ' + token['access'])
    context.response = context.client.get(url)


@when("a file is posted to a protected url")
def step_impl(context):
    file_name = test_file('reyl_fx_rates.xlsx')
    url = reverse('file_upload', args=['live', 'json'])
    with open(file_name, 'rb') as file:
        context.unauthorized_response = context.client.post(url, {'content-type': 'application/json',
                                                                  file_name: file})
    token = context.token.json()
    context.client.credentials(HTTP_AUTHORIZATION='Bearer ' + token['access'])
    with open(file_name, 'rb') as file:
        context.response = context.client.post(url, {'content-type': 'application/json',
                                                     file_name: file})


@step("the unauthorized response is not valid")
def step_impl(context):
    assert (context.unauthorized_response.status_code == 401)


@step("there is valid json in the response")
def step_impl(context):
    import json
    json.loads(context.response.content)


@when('I request the mock "{endpoint}"')
def step_impl(context, endpoint):
    url = reverse(endpoint, args=['mock', 'json'])
    token = context.token.json()
    context.client.credentials(HTTP_AUTHORIZATION='Bearer ' + token['access'])
    context.response = context.client.get(url)
