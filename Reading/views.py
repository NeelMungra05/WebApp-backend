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


@api_view(["GET"])
def target_files(request: HttpRequest) -> Response:
    s3Ops = S3Operations()
    files = s3Ops.get_target_files()

    return Response({"files": files}, status=status.HTTP_200_OK)


@api_view(["POST"])
def headers_info(request: HttpRequest) -> Response:
    source_ops = S3Operations(request, "sourceFiles")
    target_ops = S3Operations(request, "targetFiles")

    source_headers = source_ops.get_headers()
    target_headers = target_ops.get_headers()

    return Response(
        {"sourceHeaders": source_headers, "targetHeaders": target_headers},
        status=status.HTTP_200_OK,
    )
