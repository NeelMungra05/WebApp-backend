from django.http import HttpRequest

from package.Base.RequestToDict import ReqToDict
from rest_framework.request import Request
from django.http import QueryDict


class ReadFields(ReqToDict):
    def __init__(self, request: Request, attr: str) -> None:
        self.__req_cols: dict[str, list] = {}
        self.__all_cols: dict[str, list] = {}
        self.__all_pk: dict[str, list] = {}

        fields: dict = {}

        super().__init__(request, attr, "dict")

        fields = self.result if isinstance(self.result, dict) else {}

        for key in fields.keys():
            self.__req_cols[key] = [
                val for val in fields[key].keys() if fields[key][val]["RF"] == True
            ]
            self.__all_cols[key] = [val for val in fields[key].keys()]
            self.__all_pk[key] = [
                val for val in fields[key].keys() if fields[key][val]["PK"] == True
            ]

    def get_req_cols(self, fileName: str) -> list:
        return self.__req_cols[fileName]

    def get_all_cols(self, fileName: str) -> list:
        return self.__all_cols[fileName]

    def get_all_pk(self, fileName: str) -> list:
        return self.__all_pk[fileName]

    def get_req_cols_pk(self, fileName: str) -> list[list]:
        return [self.get_req_cols(fileName), self.get_all_pk(fileName)]
