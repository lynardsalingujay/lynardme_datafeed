# Generated by Django 2.2.6 on 2020-06-02 08:46

import datetime
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0018_auto_20200601_2344'),
    ]

    operations = [
        migrations.AddField(
            model_name='fundclassification',
            name='as_of',
            field=models.DateTimeField(default=datetime.datetime(2020, 6, 2, 8, 46, 12, 992041)),
            preserve_default=False,
        ),
    ]
