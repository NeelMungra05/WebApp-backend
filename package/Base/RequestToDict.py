import json

from rest_framework.request import Request
from django.http import QueryDict
from typing import Literal, Union


class ReqToDict:
    def __init__(
        self, request: Request, attr: str, type: Literal["str", "int", "dict", "list"]
    ) -> None:
        raw_result: dict = {}

        if isinstance(request.data, dict):
            raw_result = request.data

        result: Union[str, list, dict, int] = ""

        if type == "list" and isinstance(raw_result.get(attr), list):
            result = raw_result.get(attr, [])

        if type == "dict" and isinstance(raw_result.get(attr), dict):
            result = raw_result.get(attr, {})

        if type == "str" and isinstance(raw_result.get(attr), str):
            result = raw_result.get(attr, "")

        if type == "int" and isinstance(raw_result.get(attr), int):
            result = raw_result.get(attr, int)

        self.result: int | str | list | dict = result
