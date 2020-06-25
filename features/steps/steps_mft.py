from behave import *

from data.http.exante import ExanteClient
from trading.mft import calculate_signals


@given('an index called "{index}"')
def set_index(context, index):
    context.index = index


@when("I calculate the signal")
def calculate_signal(context):
    import trading.mft
    trading.mft.status = 'mock'
    df = calculate_signals(context.index)
    context.signal = df["Gross Signal"][0]


@then('it is equal to "{value}"')
def check_signal(context, value):
    signal = float(value)
    assert context.signal < signal*1.01
    assert context.signal > signal*0.99


@then("the threshold table is correctly set")
def step_impl(context):
    assert(len(context.threshold_table) == 2)
    assert(context.threshold_table['value'][0] == 0.006)


@when("I calculate the thresholds using VIX")
def step_impl(context):
    import app.page_views
    app.page_views.get_vix = lambda: 30
    context.threshold_table = app.page_views.SignalsView.get_threshold_dataframe()


@given("a sim result in the database")
def step_impl(context):
    import wsgi
    from utils.test import test_file
    from data.parse import FileParser
    file_name = test_file('sim_result_test.json')
    FileParser.parse_and_save(file_name, save_new=True)
