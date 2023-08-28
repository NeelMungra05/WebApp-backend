from django.core.files.uploadedfile import InMemoryUploadedFile, UploadedFile


class ReadFile:

    def __init__(self, inMemoryFileList: list[UploadedFile]):
        self.__fileMappingDict: dict = {}

        for f in inMemoryFileList:
            f_name = f.name
            self.__fileMappingDict[f_name] = f

        print(self.__fileMappingDict)

    def getFile(self, name: str) -> InMemoryUploadedFile:
        keys = self.__fileMappingDict.keys()

        if name not in keys:
            raise FileNotFoundError(f"Not able to find given file name:{name}")
        else:
            return self.__fileMappingDict[name]

    def getFileCount(self) -> int:
        return len(self.__fileMappingDict)

    def getFileByIdx(self, idx: int) -> InMemoryUploadedFile:
        return self.__fileMappingDict[list(self.__fileMappingDict.keys())[idx]]
