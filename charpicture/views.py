from django.http import Http404
from django.shortcuts import render

# Create your views here.
from charpicture.TransformTxt import TransformTxt
from charpicture.forms import FileUploadForm
from charpicture.models import IMG
from django.http import HttpResponse


def uploadImg(request):
    if request.method == 'POST':
        # new_img = IMG(img=request.FILES.get('img'))
        my_form = FileUploadForm(request.POST, request.FILES)
        if my_form.is_valid():
            f = my_form.cleaned_data['my_file']
            write_file(f)
            transform = TransformTxt()
            txt = handle_uploaded_file(f, transform)
        return render(request, 'charpicture/showing.html', {'img': f, 'txt': txt, 'width': transform.WIDTH, 'height': transform.HEIGHT})
    else:
        my_form = FileUploadForm()
    return render(request, 'charpicture/upload.html', {'form': my_form})


def handle_uploaded_file(f, transform):
    txt = transform.transform(f)
    return txt


def write_file(f):
    with open(f.name, 'wb+') as destination:
        for chunk in f.chunks():
            destination.write(chunk)
