from behave import *
from utils.cloud_logging import getLogger, Logger
from utils.test import CapturingStdOut

use_step_matcher("re")


@then('It is printed to stdout "(?P<truth>.+)"')
def step_impl(context, truth):
    is_expected = truth == 'True'
    is_actual = 'THIS IS A LOG MESSAGE' in context.log_capture.getvalue()
    assert(is_expected == is_actual)


@when('I log at "(?P<level>.+)"')
def step_impl(context, level):
    context.logger.log(level, 'THIS IS A LOG MESSAGE')


@given('a cloud logging configuration with "(?P<name>.+)", "(?P<use_google_logging>.+)", "(?P<is_production>.+)"')
def step_impl(context, name, use_google_logging, is_production):
    context.logger = getLogger(None, use_google_logging == 'True', is_production == 'True')


@then("It is printed to stdout")
def step_impl(context):
    assert('THIS IS A WARNING' in context.log_capture.getvalue())


@when("I log a warning")
def step_impl(context):
    context.logger.warning('THIS IS A WARNING')


@given("a dev log configuration")
def step_impl(context):
    context.logger = Logger('root', False, False)