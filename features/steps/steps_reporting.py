from behave import *
from pandas import to_datetime

from data.parse import FileParser
from pytz import utc

from app.enums import reyl

from utils.test import test_files, as_list, delete_database_data


@when('the cash reconciliation is calculated for {custodian} as of "{as_of_date}"')
def calculate_reyl_cash_rec(context, custodian, as_of_date):
    from reporting.reconciliation import cash_rec_summary
    as_of_date = to_datetime(as_of_date, utc=True)
    context.summary = cash_rec_summary(as_of_date,
                                       group_by=['currency', 'value_date', 'classification'],
                                       errors_only=True,
                                       exclude_futures=True,
                                       custodian=custodian)


@then('all the rows should be ok')
def assert_rows_are_ok(context):
    assert(len(context.summary) == 0)


@then('the trades should have net pnls (gbp) of "{net_pnl_gbps}"')
def check_pnl_summary(context, net_pnl_gbps):
    df = context.trade_summary
    net_pnl_gbps = as_list(net_pnl_gbps)
    for i, net_pnl_gbp in enumerate(net_pnl_gbps):
        x = df[df['trade_number'] == i+1]['net_pnl_gbp'].values
        assert(int(x[0]) == int(net_pnl_gbp))


@when("the trade summary is calculated")
def calculate_trade_summary(context):
    from reporting.performance import trade_summary
    context.trade_summary = trade_summary(group_by=['trade_number'], custodian=reyl)


@given("legacy Reyl data in the database")
def add_legacy_reyl_data_to_db(context):
    delete_database_data()
    for file_name in test_files('reyl_legacy', mask_include='.xlsx', mask_exclude='#'):
        FileParser.parse_and_save(file_name, save_new=True, update_existing=True)
    for file_name in test_files('reyl_legacy', mask_include='.pdf', mask_exclude='#'):
        FileParser.parse_and_save(file_name, save_new=True, update_existing=True)


@given("legacy Exante data in the database")
def add_legacy_reyl_data_to_db(context):
    for file_name in test_files('exante_test_files', mask_include='.xls|.csv'):
        FileParser.parse_and_save(file_name, save_new=True, update_existing=True)