from data.parse import FileParser
from pandas import to_datetime, DataFrame

from behave import *

from app.enums import index_future

from utils.test import test_file, group_by_field, test_files


@given('a file called "{file_name}"')
def load_file(context, file_name):
    context.file = test_file(file_name)


@when('I parse the file')
def parse_selected_file(context):
    from data.parse import FileParser
    context.models = FileParser.parse_and_save(context.file)


@then('the result should be a valid "{model}"')
def check_parse_result(context, model):
    import app.models
    model_class = getattr(app.models, model)
    objs, _ = context.models
    obj = objs[0]
    assert (isinstance(obj, model_class))


@then('There should be only one "{transaction_type}" transaction for "{symbol}" with "{value_date}", "{quantity}", '
      '"{currency}" and "{tax}"')
def check_unique_transaction(context, transaction_type, symbol, value_date, quantity, currency, tax):
    value_date, quantity, tax = to_datetime(value_date, utc=True), float(quantity), float(tax)
    correct_txs = []
    txs, message = context.models
    grouped = group_by_field(txs, 'asset_type')
    for asset_type, txs in grouped.items():
        for tx in txs:
            bools = [tx.symbol == symbol, tx.value_date == value_date, tx.quantity == quantity,
                     tx.transaction_type == transaction_type, tx.currency == currency, tx.tax == tax]
            if all(bools):
                correct_txs.append(tx)
    assert (len(correct_txs) == 1)


@then('The direct fees should be "{direct_fee}" bps and the indirect fees should be "{indirect_fee}" bps of the gross '
      'transaction value')
def check_fx_fees(context, direct_fee, indirect_fee):
    direct_fee = float(direct_fee) * 0.001
    indirect_fee = float(indirect_fee) * 0.001


@then('there are no starting or trailing spaces in the following string fields: "{fields}"')
def assert_no_trailing_spaces(context, fields):
    fields = fields.split(',')
    models, _ = context.models
    for model in models:
        for field in fields:
            value = getattr(model, field)
            assert (value[0] != ' ')
            assert (value[-1] != ' ')


@then('transactions of type "{asset_type}" have null fields "{fields}"')
def assert_null_fields(context, asset_type, fields):
    fields = fields.split(',')
    txs, _ = context.models
    for tx in txs:
        if tx.asset_type == index_future:
            for field in fields:
                value = getattr(tx, field)
                assert (value is None)


@then('there should be the same number of models after each save')
def assert_same_number_of_models(context):
    assert context.number_of_models_1 == context.number_of_models_2


@when('I parse the file and save the "{model_class}" models twice')
def parse_and_save_models_twice(context, model_class):
    from data.parse import FileParser
    import app.models
    model_class = getattr(app.models, model_class)
    FileParser.parse_and_save(context.file, save_new=True, update_existing=False)
    context.number_of_models_1 = len(model_class.objects.all())
    FileParser.parse_and_save(context.file, save_new=True, update_existing=False)
    context.number_of_models_2 = len(model_class.objects.all())


@then('There should be only one "{asset_type}" position for "{symbol}" with "{as_of_date}", "{quantity}" and "{currency}"')
def check_unique_position(context, asset_type, symbol, as_of_date, quantity, currency):
    as_of_date, quantity = to_datetime(as_of_date, utc=True), float(quantity)
    correct_pos = []
    pos, message = context.models
    grouped = group_by_field(pos, 'asset_type')
    for at, pos in grouped.items():
        for p in pos:
            bools = [p.symbol == symbol, p.as_of_date == as_of_date, p.quantity == quantity,
                     p.asset_type == at, p.currency == currency]
            if all(bools):
                correct_pos.append(p)
    assert (len(correct_pos) == 1)


@then('the "{price}", "{gross_transaction_value}" and "{net_transaction_value}" should be correct')
def assert_future_confirm(context, price, gross_transaction_value, net_transaction_value):
    df = context.contents['Transaction']
    assert isinstance(df, DataFrame)
    assert (len(df.index) == 1)
    row = df.iloc[0]
    assert (row['price'] == float(price))
    assert (row['gross_transaction_value'] == float(gross_transaction_value))
    assert (row['net_transaction_value'] == float(net_transaction_value))


@then('There should be "{quantity}" price records')
def assert_monthly_data_no_of_prices(context, quantity):
    df = context.contents['Price']
    assert isinstance(df, DataFrame)
    assert (len(df.index) == int(quantity))

@then('There should be "{quantity}" None values')
def assert_universe_data_no_of_nones(context, quantity):
    df = context.contents['Universe']
    assert isinstance(df, DataFrame)
    assert (df.isnull().sum().sum() == int(quantity))

@then('The timezone of the as_of and time column should be "{timezone}"')
def assert_monthly_5m_data_timezone(context, timezone):
    df = context.contents['Price']
    assert isinstance(df, DataFrame)
    assert (str(df['time'].dt.tz) == timezone)


@when("I parse the contents of the file")
def parse_contents(context):
    parser = FileParser.create_parser(context.file)
    context.contents = parser.parse()


@step("I have parsed the legacy Reyl transactions")
def parse_and_save_legacy_reyl_txs(context):
    from app.models import Transaction
    Transaction.objects.all().delete()
    file = test_file('reyl_legacy/reyl_transactions_legacy.xlsx')
    FileParser.parse_and_save(file, save_new=True, update_existing=True)


@then("the futures prices should be set")
def assert_no_null_futures(context):
    from app.models import Transaction
    qset = Transaction.objects.filter(price=None)
    assert (len(qset) == 0)


@when("I parse the legacy futures confirms")
def step_impl(context):
    for file in test_files('reyl_legacy', mask_include='.pdf'):
        FileParser.parse_and_save(file, save_new=True, update_existing=True)
    for file in test_files('reyl_legacy', mask_include='.json'):
        FileParser.parse_and_save(file, save_new=True, update_existing=True)


@then('the "{field}" for "{unique}" should have "{value}"')
def step_impl(context, field, unique, value):
    from app.models import Transaction
    tx = Transaction.objects.get(unique=unique)
    assert (str(getattr(tx, field)) == value)


@when('I parse the file and the the "{correcting_json_file_name}"')
def step_impl(context, correcting_json_file_name):
    FileParser.parse_and_save(test_file(context.file), save_new=True, update_existing=True)
    FileParser.parse_and_save(test_file(correcting_json_file_name), save_new=True, update_existing=True)


@then('there is only one "{model_class}" in the db')
def step_impl(context, model_class):
    import app.models
    model_cls = getattr(app.models, model_class)
    qs = model_cls.objects.all()
    n = len(qs)
    assert(n == 1)


@when('I create a "{model_class}" twice using bulk_create')
def step_impl(context, model_class):
    import app.models
    model_cls = getattr(app.models, model_class)
    model_cls.objects.bulk_create([context.price], ignore_conflicts=True)
    model_cls.objects.bulk_create([context.price], ignore_conflicts=True)


@given('a "{model_class}"')
def step_impl(context, model_class):
    import app.models
    from datetime import datetime
    from model_mommy import mommy
    #price = Price(as_of=datetime(2020, 1, 1, 12), time=datetime(2020, 1, 1, 12), value=1,
    #              source='unknown', symbol='sym', resolution='1h', asset_type='unknown',
    #              aspect='close', price_type='trade')

    context.price = mommy.make(model_class)


@step('no "{model_class}" items in the database')
def step_impl(context, model_class):
    import app.models
    model_cls = getattr(app.models, model_class)
    model_cls.objects.all().delete()
