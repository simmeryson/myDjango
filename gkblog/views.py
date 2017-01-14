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
