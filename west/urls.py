from django.conf.urls import url

from west import views

urlpatterns = [
    url(r'^$', views.first_page, name='index'),
    url(r'^staff/', views.staff),
    url(r'^templay/', views.templay),
]
