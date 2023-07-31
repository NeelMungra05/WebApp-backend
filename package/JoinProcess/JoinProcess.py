from typing import Any, Dict, List, Literal, Union, cast

import pandas as pd
from django.http import HttpRequest

from package import CustomJoins, ReadFields, ReqToDict
from package.ReadFileFromMemory import ReadFile

Join = Dict[str, List[Union[str, Dict[str, List[str]]]]]


class JoinProcess(ReqToDict):

    __joins: Join

    def __init__(self, request: HttpRequest, attr: str, joinType: Literal["sourceJoins", "targetJoins"]) -> None:
        super().__init__(request, attr)
        self.__joins = self.result[joinType]
        self.joinsResult: pd.DataFrame

    def __extract_lst_str(self, data: list[str | dict[str, list[str]]]) -> list[str]:
        return [item for item in data if isinstance(item, str)]

    def perform_operation(self, files: ReadFile, fields: ReadFields) -> None:
        joinType: list[str] = self.__extract_lst_str(self.__joins['joinType'])

        fileOrder = cast(List[str], self.__joins['fileOrder']) if isinstance(
            self.__joins['fileOrder'], list) else []

        cj = CustomJoins(fields)

        for idx, join in enumerate(joinType):

            file_idx: int = idx + 1
            joinOn_dict = cast(Dict[str, List[str]], self.__joins['joinOn'][idx]) if isinstance(
                self.__joins['joinOn'][idx], dict) else {}

            leftOn = joinOn_dict.get("leftOn", [])
            rightOn = joinOn_dict.get("rightOn", [])

            if idx == 0:
                file1 = files.getFile(fileOrder[file_idx])
                file2 = files.getFile(fileOrder[file_idx-1])

                cj.read_two_table(file1, file2)
                cj.do_joining(rightOn, leftOn,
                              how="inner" if join == "inner" else "left")
            else:
                file1 = files.getFile(fileOrder[file_idx])
                cj.read_one_table(file1)
                cj.do_joining(rightOn, leftOn,
                              how="inner" if join == "inner" else "left")

        self.joinsResult = cj.get_joined_table()

    def get_join_result(self) -> pd.DataFrame:
        return self.joinsResult
