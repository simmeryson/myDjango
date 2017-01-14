# -*- coding: utf-8 -*-
from django.conf.urls import url

from gkblog import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    # template中的url标签根据先name来匹配.
    # r''就是url地址的显示.如果url标签和name匹配上,url地址和实际传入参数可以不一样
    url(r'^home/', views.home),
    url(r'^(?P<id>\d+)/$', views.detail, name='detail'),
    url(r'^archives', views.archives, name='archives'),
    url(r'^aboutme', views.about_me, name='aboutme'),
    url(r'^tag(?P<tag>\w+)/$', views.search_tag, name='search_tag'),
]
