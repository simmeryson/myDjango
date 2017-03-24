from django.shortcuts import render
from MarkdownToHtml import MarkdownToHtml
from django.http import HttpResponse
# Create your views here.
def trans(request):
    if request.method == 'POST':
        file_input = request.FILES.get('img')
        mth = MarkdownToHtml()
        html = mth.markdownFileToHtml(file_input)
        return HttpResponse(html)
    return render(request, 'captcha/upload.html')
