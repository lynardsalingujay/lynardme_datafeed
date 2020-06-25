from time import sleep

from behave import given, when, then

from app.cache import cache_result, memoize_cache_key, get


@cache_result(timeout_s=1)
def some_function(a, b, c, d):
    return a + b + c + d


@given('a function that is decorated with "cache_result"')
def set_function(context):
    context.function = some_function
    context.args = [1, 2]
    context.kwargs = {'c': 3, 'd': 4}


@when('the function is called')
def call_function(context):
    context.result = context.function(*context.args, **context.kwargs)


@then('the result is in the cache at first but then expires')
def check_cache(context):
    cache_key = memoize_cache_key(some_function.original_function, *context.args, **context.kwargs)
    assert(get(cache_key) is not None)
    sleep(2)
    assert(get(cache_key) is None)