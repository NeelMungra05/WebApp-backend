from rest_framework.request import Request
from package.Base.RequestToDict import ReqToDict


class ReadFile(ReqToDict):
    def __init__(self, request: Request, attr: str):
        self.__fileList: list[str] = []
        super().__init__(request, attr, "list")

        self.__fileList = self.result if isinstance(self.result, list) else []

        print(self.__fileList)

    def getFileCount(self) -> int:
        return len(self.__fileList)

    def getFileByIdx(self, idx: int) -> str:
        return self.__fileList[idx]
