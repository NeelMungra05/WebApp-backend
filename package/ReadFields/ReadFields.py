from django.http import HttpRequest

from package.Base.RequestToDict import ReqToDict


class ReadFields(ReqToDict):
    __req_cols: dict[str, list] = {}
    __all_cols: dict[str, list] = {}
    __all_pk: dict[str, list] = {}

    def __init__(self, request: HttpRequest, attr: str) -> None:
        super().__init__(request, attr)

        fields: dict = self.result

        for key in fields.keys():
            self.__req_cols[key] = [
                val for val in fields[key].keys() if fields[key][val]["RF"] == True]
            self.__all_cols[key] = [val for val in fields[key].keys()]
            self.__all_pk[key] = [
                val for val in fields[key].keys() if fields[key][val]["PK"] == True]

    def get_req_cols(self, fileName: str) -> list:
        return self.__req_cols[fileName]

    def get_all_cols(self, fileName: str) -> list:
        return self.__all_cols[fileName]

    def get_all_pk(self, fileName: str) -> list:
        return self.__all_pk[fileName]

    def get_req_cols_pk(self, fileName: str) -> list[list]:
        return [self.get_req_cols(fileName), self.get_all_pk(fileName)]
