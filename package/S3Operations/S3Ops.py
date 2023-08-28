import boto3
from boto3.resources.base import ServiceResource
from django.conf import settings


class S3Operations:
    def __init__(self):
        ID = settings.ID
        KEY = settings.KEY
        REGION = settings.REGION
        self.bucket: str = settings.BUCKET

        self.session: boto3.Session = boto3.Session(
            aws_access_key_id=ID, aws_secret_access_key=KEY, region_name=REGION
        )

    def __get_files(self, bucket, prefix: str) -> list[str]:
        file_lst: list[str] = [obj.key for obj in bucket.objects.filter(Prefix=prefix)]
        update_file_list: list[str] = [
            "".join(file.split("/")[2:]) for file in file_lst
        ]
        return update_file_list

    def get_source_files(self) -> list[str]:
        s3_client: ServiceResource = self.session.resource("s3")
        bucket = s3_client.Bucket(self.bucket)

        return self.__get_files(bucket, "input/Source/")

    def get_target_files(self) -> list[str]:
        s3_client: ServiceResource = self.session.resource("s3")
        bucket = s3_client.Bucket(self.bucket)

        return self.__get_files(bucket, "input/Target/")
