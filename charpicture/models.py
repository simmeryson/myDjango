from __future__ import unicode_literals

from django.db import models


# Create your models here.
class Item(models.Model):
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ['name']

    def __unicode__(self):
        return self.name

    @models.permalink
    def get_absolute_url(self):
        return 'item_detail', None, {'object_id': self.id}


class IMG(models.Model):
    img = models.ImageField(upload_to='upload')

    def __str__(self):
        return self.title
