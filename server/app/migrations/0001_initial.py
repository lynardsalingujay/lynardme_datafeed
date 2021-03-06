# Generated by Django 2.2.3 on 2019-09-19 12:02

import app
import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='CashMovement',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('custodian', models.CharField(choices=[('unknown', 'unknown'), ('reyl', 'Reyl'), ('exante', 'Exante'), ('ubs', 'UBS'), ('selftrade', 'Selftrade'), ('interactive_brokers', 'Interactive Brokers')], default=app.enums.Custodian('unknown'), max_length=20)),
                ('owner', models.CharField(choices=[('unknown', 'unknown'), ('shiny', 'Shiny'), ('alex', 'Alex'), ('ed', 'Ed'), ('mid_pacific_am', 'Mid Pacific AM')], default=app.enums.Owner('unknown'), max_length=20)),
                ('group', models.CharField(choices=[('unknown', 'unknown'), ('shiny', 'Shiny'), ('aviva', 'Aviva'), ('mft', 'MFT')], default=app.enums.Group('unknown'), max_length=20)),
                ('transaction_date', models.DateTimeField()),
                ('value_date', models.DateTimeField()),
                ('debit_amount', models.FloatField()),
                ('credit_amount', models.FloatField()),
                ('balance', models.FloatField()),
                ('description', models.CharField(max_length=100, null=True)),
                ('currency', models.CharField(max_length=6, validators=[django.core.validators.MinLengthValidator(3)])),
                ('ledger_id', models.CharField(max_length=50, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Price',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('time', models.DateTimeField()),
                ('price', models.FloatField()),
                ('source', models.CharField(choices=[('unknown', 0), ('bloomberg', 1), ('exante', 2), ('reyl', 3), ('ft', 4)], max_length=20)),
                ('symbol', models.CharField(max_length=30, validators=[django.core.validators.MinLengthValidator(3)])),
                ('resolution', models.CharField(choices=[('unknown', 'unknown'), ('one_second', '1s'), ('one_minute', '1m'), ('one_hour', '1h'), ('one_day', '1d')], default='unknown', max_length=20)),
                ('asset_type', models.CharField(choices=[('unknown', 0), ('future', 1), ('fund', 2), ('cash', 3), ('fx', 4), ('cash_equity', 5)], default=0, max_length=20)),
            ],
        ),
        migrations.CreateModel(
            name='Transaction',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('custodian', models.CharField(choices=[('unknown', 'unknown'), ('reyl', 'Reyl'), ('exante', 'Exante'), ('ubs', 'UBS'), ('selftrade', 'Selftrade'), ('interactive_brokers', 'Interactive Brokers')], default='unknown', max_length=20)),
                ('owner', models.CharField(choices=[('unknown', 'unknown'), ('shiny', 'Shiny'), ('alex', 'Alex'), ('ed', 'Ed'), ('mid_pacific_am', 'Mid Pacific AM')], default='unknown', max_length=20)),
                ('group', models.CharField(choices=[('unknown', 'unknown'), ('shiny', 'Shiny'), ('aviva', 'Aviva'), ('mft', 'MFT')], default='unknown', max_length=20)),
                ('asset_name', models.CharField(max_length=100, null=True, validators=[django.core.validators.MinLengthValidator(3)])),
                ('transaction_time', models.DateTimeField()),
                ('value_date', models.DateTimeField()),
                ('symbol', models.CharField(max_length=20, validators=[django.core.validators.MinLengthValidator(3)])),
                ('currency', models.CharField(max_length=6, validators=[django.core.validators.MinLengthValidator(3)])),
                ('transaction_type', models.CharField(choices=[('unknown', 0), ('buy', 1), ('sell', 2), ('interest', 3), ('dividend', 4), ('fee', 5)], default='unknown', max_length=20)),
                ('asset_type', models.CharField(choices=[('unknown', 0), ('future', 1), ('fund', 2), ('cash', 3), ('fx', 4), ('cash_equity', 5)], default='unknown', max_length=20)),
                ('price', models.FloatField(null=True)),
                ('quantity', models.FloatField()),
                ('tax', models.FloatField()),
                ('direct_fee', models.FloatField()),
                ('indirect_fee', models.FloatField(null=True)),
                ('net_transaction_value', models.FloatField(null=True)),
                ('description', models.CharField(max_length=100, null=True)),
                ('gross_transaction_value', models.FloatField(null=True)),
                ('transaction_id', models.CharField(max_length=50, null=True)),
            ],
        ),
    ]
