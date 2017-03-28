from django.conf.urls import url
from rentGaode import views
urlpatterns = [
    url(r'^$', views.query, name='query'),
]