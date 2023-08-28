from django.urls import path

from .views import ReconUser

urlpatterns = [
    path("recon/", ReconUser.as_view(), name="post_recon")
]
