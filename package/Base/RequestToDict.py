import json

from django.http import HttpRequest


class ReqToDict:
    def __init__(self, request: HttpRequest, attr: str) -> None:
        str_result: str | None = request.POST.get(attr)
        self.result: dict = json.loads(str_result if str_result else " ")
