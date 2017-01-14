from django.conf.urls import url

from gkblog import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^home/', views.home),
    url(r'^(?P<id>\d+)/$', views.detail, name='detail')
]
