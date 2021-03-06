# Generated by Django 2.2.6 on 2020-05-28 21:57

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0015_price_price_type'),
    ]

    operations = [
        migrations.CreateModel(
            name='FundClassification',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('isin', models.CharField(max_length=50)),
                ('index', models.CharField(max_length=50)),
                ('fx', models.CharField(max_length=50)),
                ('has_currency_hedge', models.BooleanField()),
                ('index_offset', models.IntegerField()),
                ('fx_time', models.TimeField()),
                ('start_date', models.DateField()),
                ('end_date', models.DateField()),
                ('include_holidays', models.BooleanField()),
                ('rsq', models.FloatField()),
                ('beta', models.FloatField()),
                ('approved', models.BooleanField()),
            ],
        ),
        migrations.AlterField(
            model_name='position',
            name='asset_type',
            field=models.CharField(choices=[('unknown', 'unknown'), ('index_future', 'index_future'), ('fund', 'fund'), ('cash', 'cash'), ('fx_spot', 'fx_spot'), ('cash_equity', 'cash_equity'), ('fx_forward', 'fx_forward'), ('fx_future', 'fx_future'), ('equity_index', 'equity_index'), ('volatility_index', 'volatility_index')], default='unknown', max_length=20),
        ),
        migrations.AlterField(
            model_name='price',
            name='asset_type',
            field=models.CharField(choices=[('unknown', 'unknown'), ('index_future', 'index_future'), ('fund', 'fund'), ('cash', 'cash'), ('fx_spot', 'fx_spot'), ('cash_equity', 'cash_equity'), ('fx_forward', 'fx_forward'), ('fx_future', 'fx_future'), ('equity_index', 'equity_index'), ('volatility_index', 'volatility_index')], default='unknown', max_length=20),
        ),
        migrations.AlterField(
            model_name='price',
            name='source',
            field=models.CharField(choices=[('unknown', 'unknown'), ('Bloomberg', 'Bloomberg'), ('Exante', 'Exante'), ('Reyl', 'Reyl'), ('Ft', 'Ft'), ('PortaraCQG', 'PortaraCQG'), ('Dukascopy', 'Dukascopy')], max_length=20),
        ),
        migrations.AlterField(
            model_name='transaction',
            name='asset_type',
            field=models.CharField(choices=[('unknown', 'unknown'), ('index_future', 'index_future'), ('fund', 'fund'), ('cash', 'cash'), ('fx_spot', 'fx_spot'), ('cash_equity', 'cash_equity'), ('fx_forward', 'fx_forward'), ('fx_future', 'fx_future'), ('equity_index', 'equity_index'), ('volatility_index', 'volatility_index')], default='unknown', max_length=20),
        ),
    ]
