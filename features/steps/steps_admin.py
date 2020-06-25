from behave import *

use_step_matcher("re")


@given("a django application")
def step_impl(context):
    pass


@then("there should be no failures")
def step_impl(context):
    assert('System check identified no issues' in str(context.result.stdout))


@when("I run check")
def step_impl(context):
    import subprocess
    context.result = subprocess.run(["django-admin", "check"], stdout=subprocess.PIPE)


@then("there should be no new migrations")
def step_impl(context):
    assert('No changes detected' in str(context.result.stdout))


@when("I run makemigrations")
def step_impl(context):
    import subprocess
    context.result = subprocess.run(["django-admin", "makemigrations", "--dry-run"], stdout=subprocess.PIPE)
