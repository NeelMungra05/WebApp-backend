from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import HttpRequest
from rest_framework.response import Response

from package.S3Operations import S3Operations

# Create your views here.


@api_view(["GET"])
def source_files(request: HttpRequest) -> Response:
    s3Ops = S3Operations()
    files = s3Ops.get_source_files()

    return Response({"files": files}, status=status.HTTP_200_OK)
