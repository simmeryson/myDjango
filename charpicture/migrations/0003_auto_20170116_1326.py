# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2017-01-16 13:26
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('charpicture', '0002_filesimplemodel'),
    ]

    operations = [
        migrations.AlterField(
            model_name='filesimplemodel',
            name='file_field',
            field=models.FileField(upload_to='upload-%Y-%m-%d'),
        ),
    ]