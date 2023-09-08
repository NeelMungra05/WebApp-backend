from io import BytesIO
from typing import Optional

import boto3
import pandas as pd
from boto3.resources.base import ServiceResource
from django.conf import settings
from pandas import DataFrame
from rest_framework.request import Request

from package import ReqToDict


class S3Operations(ReqToDict):
    def __init__(self, request: Optional[Request] = None, parmam: Optional[str] = None):
        self.__fileNames: Optional[dict[str, list]] = None
        if request is not None and parmam is not None:
            super().__init__(request, parmam, "dict")
            self.__fileNames = self.result if isinstance(self.result, dict) else {}

        ID = settings.ID
        KEY = settings.KEY
        REGION = settings.REGION
        self.bucket: str = settings.BUCKET

        self.session: boto3.Session = boto3.Session(
            aws_access_key_id=ID, aws_secret_access_key=KEY, region_name=REGION
        )

    def __get_files(self, bucket, prefix: str) -> list[dict[str, int | str]]:
        file_lst: list[dict[str, int | str]] = [
            {"name": obj.key, "size": obj.size}
            for obj in bucket.objects.filter(Prefix=prefix)
        ]
        # update_file_list: list[str] = [
        #     "".join(file.split("/")[2:]) for file in file_lst
        # ]
        return file_lst

    def get_source_files(self) -> list[dict[str, int | str]]:
        s3_client: ServiceResource = self.session.resource("s3")
        bucket = s3_client.Bucket(self.bucket)  # type: ignore

        return self.__get_files(bucket, "input/Source/")

    def get_target_files(self) -> list[dict[str, int | str]]:
        s3_client: ServiceResource = self.session.resource("s3")
        bucket = s3_client.Bucket(self.bucket)  # type:ignore
        return self.__get_files(bucket, "input/Target/")

    def __read_files_from_s3(
        self, name: str, s3_client, cols: Optional[list[str]] = None
    ) -> pd.DataFrame:
        response = s3_client.get_object(Bucket=self.bucket, Key=name)
        io_data = response["Body"].read()
        df: DataFrame = pd.read_excel(BytesIO(io_data), usecols=cols)

        return df

    def get_headers(self) -> dict[str, list]:
        s3_client = self.session.client("s3")

        result: dict[str, list] = {}

        if self.__fileNames is not None:
            for file in self.__fileNames:
                result[file] = self.__read_files_from_s3(
                    file, s3_client
                ).columns.to_list()

        return result

    def read_files(self, name: str, cols: Optional[list[str]] = None) -> pd.DataFrame:
        s3_client = self.session.client("s3")

        result: pd.DataFrame = pd.DataFrame()

        result = self.__read_files_from_s3(name, s3_client, cols)

        return result
