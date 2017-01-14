from __future__ import unicode_literals

from django.db import models

# Create your models here.
from django.urls import reverse


class Article(models.Model):
    title = models.CharField(max_length=100)
    category = models.CharField(max_length=50, blank=True)
    date_time = models.DateTimeField(auto_now_add=True)
    content = models.TextField(blank=True, null=True)

    # def get_absolute_url(self):
    #     return reverse('issue_detail', kwargs={'pk': self.pk})

    def __str__(self):
        return self.title
