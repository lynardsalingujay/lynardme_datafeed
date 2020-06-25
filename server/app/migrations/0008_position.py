# Generated by Django 2.2.6 on 2019-10-31 01:30

import app.enums
import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0006_auto_20190925_1313'),
    ]

    operations = [
        migrations.CreateModel(
            name='Position',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('custodian', models.CharField(choices=[('unknown', 'unknown'), ('reyl', 'Reyl'), ('exante', 'Exante'), ('ubs', 'UBS'), ('selftrade', 'Selftrade'), ('interactive_brokers', 'Interactive Brokers')], default=app.enums.Custodian('unknown'), max_length=20)),
                ('owner', models.CharField(choices=[('unknown', 'unknown'), ('shiny', 'Shiny'), ('alex', 'Alex'), ('ed', 'Ed'), ('mid_pacific_am', 'Mid Pacific AM')], default=app.enums.Owner('unknown'), max_length=20)),
                ('group', models.CharField(choices=[('unknown', 'unknown'), ('shiny', 'Shiny'), ('aviva', 'Aviva'), ('mft', 'MFT')], default=app.enums.Group('unknown'), max_length=20)),
                ('value_date', models.DateTimeField(null=True)),
                ('as_of_date', models.DateTimeField()),
                ('symbol', models.CharField(max_length=30, validators=[django.core.validators.MinLengthValidator(3)])),
                ('currency', models.CharField(max_length=6, validators=[django.core.validators.MinLengthValidator(3)])),
                ('asset_type', models.CharField(choices=[('unknown', 0), ('future', 1), ('fund', 2), ('cash', 3), ('fx', 4), ('cash_equity', 5)], default=0, max_length=20)),
                ('quantity', models.FloatField()),
                ('unique', models.CharField(max_length=50, null=True, unique=True)),
            ],
        ),
    ]