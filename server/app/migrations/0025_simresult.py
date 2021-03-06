# Generated by Django 2.2.6 on 2020-06-18 12:51

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0024_auto_20200612_1110'),
    ]

    operations = [
        migrations.CreateModel(
            name='SimResult',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('as_of', models.DateTimeField()),
                ('open_threshold_beta', models.FloatField()),
                ('open_threshold_alpha', models.FloatField()),
                ('close_threshold_beta', models.FloatField()),
                ('close_threshold_alpha', models.FloatField()),
                ('geo', models.CharField(max_length=20, null=True)),
                ('index', models.CharField(max_length=20, null=True)),
                ('volatility_index', models.CharField(max_length=20, null=True)),
                ('start_date', models.DateTimeField()),
                ('end_date', models.DateTimeField()),
                ('fund', models.CharField(max_length=20, null=True)),
                ('min_holding_days', models.IntegerField()),
                ('dealing_cutoff_time_london', models.TimeField()),
                ('is_live', models.BooleanField()),
            ],
        ),
    ]
