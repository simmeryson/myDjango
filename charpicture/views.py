from django.core.files import File
from django.http import Http404
from django.shortcuts import render

# Create your views here.
from charpicture.TransformTxt import TransformTxt
from charpicture.forms import FileUploadForm
from charpicture.models import IMG, FileSimpleModel
from django.http import HttpResponse


def uploadImg(request):
    if request.method == 'POST':
        new_img = IMG(img=request.FILES.get('img'))
        new_img.save()
        imgs = IMG.objects.all()
        print imgs
        my_form = FileUploadForm(request.POST, request.FILES)
        if my_form.is_valid():
            f = my_form.cleaned_data['my_file']
            # file_model = FileSimpleModel()
            # file_model.file_field = my_form.cleaned_data['my_file']
            # file_model.save()
            # imgs = FileSimpleModel.objects.all()
            # files = map((lambda x: File(x.file_field)), imgs)
            # write_file(f)
            transform = TransformTxt()
            txt = handle_uploaded_file(f, transform)
        return render(request, 'charpicture/showing.html',
                      {'imgs': imgs, 'txt': txt, 'width': transform.WIDTH, 'height': transform.HEIGHT})
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
