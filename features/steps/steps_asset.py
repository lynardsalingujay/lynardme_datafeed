from behave import given, when, then

from app.asset import Asset


@given('"{symbol_in}", "{source_in}", "{security_type}" and "{source_out}"')
def set_security_inputs(context, symbol_in, source_in, security_type, source_out):
    context.symbol_in = symbol_in
    context.source_in = source_in
    context.security_type = security_type
    context.source_out = source_out


@when('I convert the symbol')
def parse_and_unparse_security(context):
    security = Asset.create_asset(context.symbol_in, context.security_type, context.source_in)
    context.symbol_out = security.to_symbol(context.source_out)


@then('I get "{symbol_out}"')
def compare_symbols(context, symbol_out):
    assert(context.symbol_out == symbol_out)


@then("All the CashMovement models have valid classifications")
def step_impl(context):
    txs, message = context.models
    classifications = []
    for cash_movement in txs:
        classifications.append(cash_movement.classification)


@given('a cash movement description "{description}"')
def step_impl(context, description):
    context.description = description


@then('the classification should be "{classification}"')
def step_impl(context, classification):
    if context.classification != classification:
        raise ValueError(context.description + ': expected ' + classification + ', got ' + context.classification)


@when("I classify the cash movement")
def step_impl(context):
    from app.models import CashMovement
    context.classification = CashMovement._classification(context.description)


@then('"{name}" should be "{value}"')
def step_impl(context, name, value):
    my_val = getattr(context.enums, name)
    assert(my_val == value)


@given("I import the enums module")
def step_impl(context):
    import app.enums
    context.enums = app.enums
