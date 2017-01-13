from django.http import HttpResponse
from django.shortcuts import render


def first_page(Request):
    return HttpResponse("<p>nihao</p>")
