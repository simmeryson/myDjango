# -*- coding: utf-8 -*-
from django.contrib.syndication.views import Feed
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage
from django.http import Http404
from django.http import HttpResponse
from django.shortcuts import render, redirect

# Create your views here.
from django.urls import reverse

from gkblog.models import Article


def index(requese):
    return HttpResponse("<p>Welcome GKblog </p>")


def home(request):
    post = Article.objects.all()
    paginator = Paginator(post, 1) #每页显示1个
    page = request.GET.get('page')
    try:
        post_list = paginator.page(page)
    except PageNotAnInteger:
        post_list = paginator.page(1)
    except EmptyPage:
        post_list = paginator.paginator(paginator.num_pages)

    return render(request, 'gkblog/home.html', {'post_list': post_list})


def detail(request, id):
    try:
        post = Article.objects.get(id=str(id))
    except:
        raise Http404
    return render(request, 'gkblog/post.html', {'post': post})


def archives(request):
    try:
        post_list = Article.objects.all()
    except Article.DoesNotExist:
        raise Http404
    return render(request, 'gkblog/archives.html', {'post_list': post_list, 'error': False})


def about_me(request):
    return render(request, 'gkblog/aboutme.html')


def search_tag(request, tag):
    try:
        post_list = Article.objects.filter(category__iexact=tag)
    except Article.DoesNotExist:
        raise Http404
    return render(request, 'gkblog/tag.html', {'post_list': post_list})


def bolg_search(request):
    if 's' in request.GET:
        s = request.GET['s']
        if not s:
            return render(request, 'gkblog/home.html')
        else:
            post_list = Article.objects.filter(title__icontains=s)
            if len(post_list) == 0:
                return render(request, 'gkblog/archives.html', {'post_list': post_list, 'error': True})
            else:
                return render(request, 'gkblog/archives.html', {'post_list': post_list, 'error': False})
    return redirect('/')


class RSSFeed(Feed):
    title = "RSS feed - article"
    link = "feed/posts/"
    description = "RSS feed - blog posts"

    def items(self):
        return Article.objects.order_by('-date_time')

    def item_title(self, item):
        return item.title

    def item_pubdate(self, item):
        return item.add_date

    def item_description(self, item):
        return item.content

    # item_link is only needed if NewsItem has no get_absolute_url method.
    def item_link(self, item):
        return reverse('article-item', args=[item.pk])
