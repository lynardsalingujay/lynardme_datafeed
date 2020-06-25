from django.core.validators import MinLengthValidator
from django.db.models import Model, UniqueConstraint
from django.db.models.fields import *
from pandas import DataFrame

from app.enums import *
from app.enums import fund, index_future, AssetType, fx_forward, fx_spot, TransactionType, Source, Resolution, Custodian, \
    Owner, Group, transfer, interest, dividend, fee, Aspect, PriceType

from django.core.exceptions import ObjectDoesNotExist
import sys

this_module = sys.modules[__name__]


def model_fields(model):
    return [field.name for field in model.__class__._meta.get_fields()]


def to_dict(model):
    fields = model_fields(model)
    as_dict = dict()
    for field in fields:
        as_dict[field] = getattr(model, field)
    return as_dict


def to_dataframe(models, *additional_fields):
    records = []
    for model in models:
        as_dict = to_dict(model)
        for field in additional_fields:
            as_dict[field] = getattr(model, field)
        records.append(as_dict)
    return DataFrame.from_records(records)


def field_names(model_cls):
    names = [field.name for field in model_cls._meta.get_fields() if field.name != 'id']
    return names


def to_models(model_cls, df):
    cols = [col for col in field_names(model_cls) if col in list(df.columns.values)]
    df = df[cols]
    data = df.to_dict(orient='records')
    objects = []
    for d in data:
        obj = model_cls(**d)
        objects.append(obj)
    return objects, cols


def to_json(models, *additional_fields):
    df = to_dataframe(models, *additional_fields)
    return df.to_json(orient='records', date_format='iso')


#### NULL FIELDS CANNOT BE USED IN THE UNIQUE CONSTRAINT ####
#### MAKE SURE YOU CREATE THE MIGRATIONS BEFORE RUNNING UNIT TESTS ####

bulk_update_fields = {'FundClassification': ['fund', 'index', 'fx', 'has_currency_hedge', 'index_offset', 'fx_time',
                                                'start_date', 'end_date', 'include_holidays', 'rsq', 'beta', 'approved'],
                      'Position': ['custodian', 'owner', 'group', 'as_of_date', 'value_date',
                                                'currency', 'unique', 'symbol', 'asset_type'],
                      'CashMovement': ['custodian', 'owner', 'group', 'transaction_date', 'value_date',
                                                'currency', 'description'],
                      'Transaction': ['custodian', 'owner', 'group', 'transaction_time',
                                                'value_date', 'symbol', 'currency', 'transaction_type', 'asset_type'],
                      'Price': ['time', 'source', 'symbol', 'resolution', 'asset_type', 'aspect', 'price_type'],
                      'Universe': ['symbol', 'as_of']}


def update_unique(cls_name, d, unique_fields=None):
    cls = getattr(this_module, cls_name)
    if 'unique' in d and d['unique']:
        qs = cls.objects.filter(unique=d['unique'])
    else:
        if unique_fields is None:
            unique_fields_ = bulk_update_fields[cls_name]
        else:
            unique_fields_ = unique_fields
        unique_data = {field: d[field] for field in unique_fields_ if field in d}
        qs = cls.objects.filter(**unique_data)
    if len(qs) == 0:
        pass
    elif len(qs) == 1:
        update_cols = [col for col in field_names(cls) if col in d.keys()]
        update_data = {field: d[field] for field in update_cols if field in d}
        qs.update(**update_data)
    else:
        raise ValueError('update_unique: existing non-unique objects found')


class Price(Model):
    as_of = DateTimeField()
    time = DateTimeField(db_index=True)
    value = FloatField()
    source = CharField(max_length=20, choices=Source.choices())
    symbol = CharField(validators=[MinLengthValidator(3)], max_length=30, db_index=True)
    resolution = CharField(max_length=20, choices=Resolution.choices(), default=Resolution.choices()[0][0], db_index=True)
    asset_type = CharField(max_length=20, choices=AssetType.choices(), default=AssetType.choices()[0][0])
    aspect = CharField(max_length=20, choices=Aspect.choices(), default=Aspect.choices()[0][0], db_index=True)
    price_type = CharField(max_length=20, choices=PriceType.choices(), default=PriceType.choices()[0][0], db_index=True)

    class Meta:
        constraints = [UniqueConstraint(name='unique_price',
                                        fields=bulk_update_fields['Price'])]


class Universe(Model):
    as_of = DateTimeField() ##
    expiry_date = DateTimeField(null=True)
    bb_ticker = CharField(max_length=20) ##
    asset_type = CharField(max_length=20, choices=AssetType.choices(), default=AssetType.choices()[0][0])
    contract_mul = FloatField(null=True)
    currency = CharField(max_length=20)
    underlying_index = CharField(max_length=20, null=True)
    isin = CharField(max_length=20, null=True)
    name = CharField(max_length=50)
    symbol = CharField(validators=[MinLengthValidator(3)], max_length=30, db_index=True) ##
    fund_size = FloatField(null=True)
    fund_size_currency = CharField(max_length=20, null=True)
    front_load = FloatField(null=True)
    back_load = FloatField(null=True)
    min_investment = FloatField(null=True)
    performance_fee = FloatField(null=True)
    geo = CharField(max_length=20, null=True)

    class Meta:
        constraints = [UniqueConstraint(name='unique_universe',
                                        fields=bulk_update_fields['Universe'])]


class SimResult(Model):
    as_of = DateTimeField()
    open_threshold_beta = FloatField()
    open_threshold_alpha = FloatField()
    close_threshold_beta = FloatField()
    close_threshold_alpha = FloatField()
    geo = CharField(max_length=20, null=True)
    index = CharField(max_length=20, null=True)
    volatility_index = CharField(max_length=20, null=True)
    start_date = DateTimeField()
    end_date = DateTimeField()
    fund = CharField(max_length=20, null=True)
    min_holding_days = IntegerField()
    dealing_cutoff_time_london = TimeField()
    is_live = BooleanField()


class Transaction(Model):
    custodian = CharField(max_length=20, choices=Custodian.choices(), default=Custodian.choices()[0][0])
    owner = CharField(max_length=20, choices=Owner.choices(), default=Owner.choices()[0][0])
    group = CharField(max_length=20, choices=Group.choices(), default=Group.choices()[0][0])
    asset_name = CharField(validators=[MinLengthValidator(3)], max_length=100, null=True)
    transaction_time = DateTimeField()
    value_date = DateTimeField()
    symbol = CharField(validators=[MinLengthValidator(3)], max_length=30)
    currency = CharField(validators=[MinLengthValidator(3)], max_length=6)
    transaction_type = CharField(max_length=20, choices=TransactionType.choices(),
                                 default=TransactionType.choices()[0][0])
    asset_type = CharField(max_length=20, choices=AssetType.choices(), default=AssetType.choices()[0][0])
    price = FloatField(null=True)
    quantity = FloatField()
    tax = FloatField()
    direct_fee = FloatField()
    indirect_fee = FloatField(null=True)
    net_transaction_value = FloatField(null=True)
    description = TextField(null=True)
    gross_transaction_value = FloatField(null=True)
    unique = CharField(max_length=50, null=True, unique=True)

    class Meta:
        constraints = [UniqueConstraint(name='unique_transaction',
                                        fields=bulk_update_fields['Transaction'])]


class CashMovement(Model):
    custodian = CharField(max_length=20, choices=Custodian.choices(), default=Custodian.choices()[0][0])
    owner = CharField(max_length=20, choices=Owner.choices(), default=Owner.choices()[0][0])
    group = CharField(max_length=20, choices=Group.choices(), default=Group.choices()[0][0])
    transaction_date = DateTimeField()
    value_date = DateTimeField()
    debit_amount = FloatField()
    credit_amount = FloatField()
    balance = FloatField()
    description = TextField(null=True)
    currency = CharField(validators=[MinLengthValidator(3)], max_length=6)
    unique = CharField(max_length=50, null=True, unique=True)

    class Meta:
        constraints = [UniqueConstraint(name='unique_cash_movement',
                                        fields=bulk_update_fields['CashMovement'])]

    @staticmethod
    def _classification(description):
        if description[:7] in ['Subscr.', 'Repurch']:
            return fund
        elif description[:13] in ['Corr. Subscr.', 'Corr. Repurch']:
            return fund
        elif description[:4] in ['Your']:
            words = description.split(' ')
            if len(words) == 5:
                return fx_forward
            elif len(words) == 4:
                return fx_spot
        elif 'future' in description.lower():
            return index_future
        elif 'futures' in description.lower():
            return index_future
        elif description[:16] == 'ajustement marge':
            return index_future
        elif 'variation de marge' in description:
            return index_future
        elif description[:4] == 'Sale':
            return index_future
        elif description[:8] == 'Purchase':
            return index_future
        elif 'TRANSFER' in description:
            return transfer
        elif 'NO.'in description:
            return transfer
        elif 'interest' in description:
            return interest
        elif 'Cash distrib. ' in description:
            return dividend
        elif 'Administration Fee' in description:
            return fee
        elif 'Commercial' in description:
            return fee
        elif 'Custody fee' in description:
            return fee
        else:
            raise ValueError('Cannot classify description='+description)

    @property
    def classification(self):
        return self._classification(self.description)


class Position(Model):
    custodian = CharField(max_length=20, choices=Custodian.choices(), default=Custodian.choices()[0][0])
    owner = CharField(max_length=20, choices=Owner.choices(), default=Owner.choices()[0][0])
    group = CharField(max_length=20, choices=Group.choices(), default=Group.choices()[0][0])
    value_date = DateTimeField(null=True)
    as_of_date = DateTimeField()
    symbol = CharField(validators=[MinLengthValidator(3)], max_length=30)
    currency = CharField(validators=[MinLengthValidator(3)], max_length=6)
    asset_type = CharField(max_length=20, choices=AssetType.choices(), default=AssetType.choices()[0][0])
    quantity = FloatField()
    unique = CharField(max_length=50, null=True, unique=True)

    class Meta:
        constraints = [UniqueConstraint(name='unique_position',
                                        fields=bulk_update_fields['Position'])]


class FundClassification(Model):
    # inputs
    fund = CharField(max_length=50)
    index = CharField(max_length=50)
    fx = CharField(max_length=50)
    has_currency_hedge = BooleanField()
    index_offset = IntegerField()
    fx_time = TimeField()
    start_date = DateField()
    end_date = DateField()
    include_holidays = BooleanField()
    # outputs
    rsq = FloatField()
    beta = FloatField()
    approved = BooleanField()
    as_of = DateTimeField()

    class Meta:
        constraints = [UniqueConstraint(name='unique_fund_classification',
                                        fields=bulk_update_fields['FundClassification'])]
