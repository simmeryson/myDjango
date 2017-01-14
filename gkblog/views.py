from django.http import Http404
from django.http import HttpResponse
from django.shortcuts import render

# Create your views here.
from gkblog.models import Article


def index(requese):
    return HttpResponse("<p>Welcome GKblog </p>")


def home(request):
    post_list = Article.objects.all()
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
