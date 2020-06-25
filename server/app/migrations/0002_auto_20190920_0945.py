# Generated by Django 2.2.3 on 2019-09-20 09:45

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='transaction',
            name='symbol',
            field=models.CharField(max_length=30, validators=[django.core.validators.MinLengthValidator(3)]),
        ),
    ]