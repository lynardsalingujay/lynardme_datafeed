import wsgi
from behave import fixture, use_fixture
import django
from django.test.runner import DiscoverRunner
from django.test.testcases import LiveServerTestCase
from rest_framework.test import APIClient


def before_all(context):
    use_fixture(django_test_runner, context)
    context.client = APIClient()


@fixture
def django_test_runner(context):
    django.setup()
    context.test_runner = DiscoverRunner()
#    context.test_runner.setup_test_environment()
    context.old_db_config = context.test_runner.setup_databases()
    yield
    context.test_runner.teardown_databases(context.old_db_config)
#    context.test_runner.teardown_test_environment()


@fixture
def django_test_case(context):
    context.test_case = LiveServerTestCase
    context.test_case.setUpClass()
    yield
    context.test_case.tearDownClass()
    del context.test_case


def before_scenario(context, scenario):
    use_fixture(django_test_case, context)
