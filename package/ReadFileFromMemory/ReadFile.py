class ReadFile:
    __fileMappingDict: dict = {}

    def __init__(self, inMemoryFileList):

        for f in inMemoryFileList:
            f_name = f.name
            self.__fileMappingDict[f_name] = f

    def getFile(self, name: str):
        keys = self.__fileMappingDict.keys()

        if name not in keys:
            raise FileNotFoundError(f"Not able to find given file name:{name}")
        else:
            return self.__fileMappingDict[name]
