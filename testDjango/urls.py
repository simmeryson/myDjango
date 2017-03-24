# -*- coding: utf-8 -*-
"""testDjango URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.10/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""

from django.conf.urls import url, include
from django.contrib import admin

from testDjango import views

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^$', views.first_page),
    url(r'^west/', include('west.urls')),
    url(r'^gkblog/', include('gkblog.urls')),
    url(r'^charpic/', include('charpicture.urls')),
    url(r'^captcha/', include('captcha.urls')),
    url(r'^ticket/', include('ticket.urls')),
    url(r'^summary/', include('newssummary.urls')),
    url(r'^markdown/', include('markdownHtml.urls')),
    #include语法相当于二级路由策略，它将接收到的url地址去除了它前面的正则表达式，将剩下的字符串传递给下一级路由进行判断
    #regex不会去匹配GET或POST参数或域名，例如对于https://www.example.com/myapp， regex只尝试匹配myapp/。对于https://www.example.com/myapp/?page=3， regex也只尝试匹配myapp/
]
