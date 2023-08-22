from django.urls import path

from Reading.views import source_files

urlpatterns = [path("list/source/", source_files, name="source_file_list")]
