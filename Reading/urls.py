from django.urls import path

from Reading.views import source_files, target_files

urlpatterns = [
    path("list/source/", source_files, name="source_file_list"),
    path("list/target/", target_files, name="target_file_list"),
]
