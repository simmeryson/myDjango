from PIL import Image
from django.shortcuts import render

# Create your views here.
from captcha.Recognition import Recognition


def upload(request):
    if request.method == 'POST':
        file_input = request.FILES.get('img')
        reg = Recognition()
        img = Image.open(file_input)
        output = reg.input_image(img)
        return render(request, 'captcha/showing.html', {'post': "".join([i[1] for i in output])})
    return render(request, 'captcha/upload.html')
