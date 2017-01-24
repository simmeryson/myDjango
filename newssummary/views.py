from django.shortcuts import render

# Create your views here.
import news_summary


def summary(request):
    if request.method == 'POST':
        file_txt = request.FILES.get('txt')
        text = ''
        for s in file_txt.readlines():
            text += s.replace('\n', '')
        post_list = news_summary.Summarize(text, 2)
    return render(request, 'newssummary/input.html', {'post_list':post_list})
