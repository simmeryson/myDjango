# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2017-01-16 13:11
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('charpicture', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='FileSimpleModel',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('file_field', models.FileField(upload_to='upload/%Y/%m/%d')),
            ],
        ),
    ]
