from django.conf.urls import url

from ticket import views

urlpatterns = [
    url(r'^$', views.query, name='query'),
]
