# Generated by Django 2.2.6 on 2020-06-02 23:03

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0017_auto_20200529_1312'),
    ]

    operations = [
        migrations.AddConstraint(
            model_name='cashmovement',
            constraint=models.UniqueConstraint(fields=('custodian', 'owner', 'group', 'transaction_date', 'value_date', 'currency', 'unique'), name='unique_cash_movement'),
        ),
        migrations.AddConstraint(
            model_name='fundclassification',
            constraint=models.UniqueConstraint(fields=('fund', 'index', 'fx', 'has_currency_hedge', 'index_offset', 'fx_time', 'start_date', 'end_date', 'include_holidays', 'rsq', 'beta', 'approved'), name='unique_fund_classification'),
        ),
        migrations.AddConstraint(
            model_name='position',
            constraint=models.UniqueConstraint(fields=('custodian', 'owner', 'group', 'as_of_date', 'value_date', 'currency', 'unique', 'symbol', 'asset_type'), name='unique_position'),
        ),
        migrations.AddConstraint(
            model_name='price',
            constraint=models.UniqueConstraint(fields=('time', 'source', 'symbol', 'resolution', 'asset_type', 'aspect', 'price_type'), name='unique_price'),
        ),
        migrations.AddConstraint(
            model_name='transaction',
            constraint=models.UniqueConstraint(fields=('custodian', 'owner', 'group', 'asset_name', 'transaction_time', 'value_date', 'symbol', 'currency', 'transaction_type', 'asset_type', 'unique'), name='unique_transaction'),
        ),
    ]
