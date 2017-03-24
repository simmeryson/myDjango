from django.conf.urls import url

from markdownHtml import views

urlpatterns = [
    url(r'^$', views.trans, name='summary'),
]