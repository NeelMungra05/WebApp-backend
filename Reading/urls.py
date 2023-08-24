from django.urls import path

from Reading.views import headers_info, source_files, target_files

urlpatterns = [
    path("list/source/", source_files, name="source_file_list"),
    path("list/target/", target_files, name="target_file_list"),
    path("headers/", headers_info, name="headers_list"),
]
