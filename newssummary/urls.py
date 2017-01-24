from django.conf.urls import url

from newssummary import views

urlpatterns = [
    url(r'^$', views.summary, name='summary'),
]
