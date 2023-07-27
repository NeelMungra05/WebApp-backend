from typing import Literal

import pandas as pd
from django.core.files.uploadedfile import InMemoryUploadedFile
from django.http import HttpRequest

from package.ReadFields.ReadFields import ReadFields


class CustomJoins:
    __df: pd.DataFrame | None
    __table1: pd.DataFrame | None
    __table2: pd.DataFrame | None

    def __init__(self, fields: ReadFields) -> None:
        self.__fields: ReadFields = fields
        self.__df = None

    def __erase_table(self) -> None:
        self.__table1 = None
        self.__table2 = None

    def __read_table(self, file: InMemoryUploadedFile) -> pd.DataFrame:
        print(file)
        cols: list[str] = self.__fields.get_req_cols(
            fileName=file.name)
        return pd.read_excel(file, usecols=cols)

    def read_two_table(self, file1: InMemoryUploadedFile, file2: InMemoryUploadedFile) -> None:
        self.__table1 = self.__read_table(file1)
        self.__table2 = self.__read_table(file2)

    def read_one_table(self, file: InMemoryUploadedFile) -> None:
        self.__table2 = self.__read_table(file)

    def do_joining(self, right_on: list[str], left_on: list[str], how: Literal["left", "inner"]) -> None:

        if self.__table1 is not None and self.__table2 is not None:
            if self.__df is None:
                self.__table1
                self.__df = pd.merge(self.__table1, self.__table2,
                                     left_on=left_on, right_on=right_on, how=how)
            else:
                self.__df = self.__df.merge(
                    self.__table2, left_on=left_on, right_on=right_on, how=how)

    def get_joined_table(self) -> pd.DataFrame:
        return self.__df if self.__df is not None else pd.DataFrame()
