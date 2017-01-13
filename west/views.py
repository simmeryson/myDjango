from django.shortcuts import render, render_to_response

# Create your views here.

from django.http import HttpResponse

from west.models import Character


def first_page(Request):
    return HttpResponse("<p>west world!</p>")


def staff(Request):
    staff_list = Character.objects.all()
    staff_str = map(str, staff_list)
    return HttpResponse("<p>" + ' '.join(staff_str) + "</p>")


def templay(request):
    context = {}
    context['label'] = 'Hello West!'
    return render(request, 'west/templay.html', context)
    # return render_to_response('templay.html', {'label': 'Hello West'})

