# Generated by Django 2.2.6 on 2020-05-29 13:12

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('app', '0016_auto_20200528_2157'),
    ]

    operations = [
        migrations.RenameField(
            model_name='fundclassification',
            old_name='isin',
            new_name='fund',
        ),
    ]
