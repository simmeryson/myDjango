from django.forms import forms


class FileUploadForm(forms.Form):
    my_file = forms.FileField()
