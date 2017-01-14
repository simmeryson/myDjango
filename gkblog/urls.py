# -*- coding: utf-8 -*-
from django.conf.urls import url

from gkblog import views
from gkblog.views import RSSFeed

urlpatterns = [
    url(r'^$', views.index, name='index'),
    # template中的url标签根据先name来匹配.
    # r''就是url地址的显示.如果url标签和name匹配上,url地址和实际传入参数可以不一样
    url(r'^home/', views.home, name="home"),
    url(r'^(?P<id>\d+)/$', views.detail, name='detail'),
    url(r'^archives', views.archives, name='archives'),
    url(r'^about_me', views.about_me, name='about_me'),
    url(r'^tag(?P<tag>\w+)/$', views.search_tag, name='search_tag'),
    url(r'^search/$', views.bolg_search, name='search'),
    url(r'^feed/$', RSSFeed(), name="RSS"),  # 新添加的urlconf, 并将name设置为RSS, 方便在模板中使用url
]
