class ReadFile:
    _fileMappingDict: dict = {}

    def __init__(self, inMemoryFileList):

        for f in inMemoryFileList:
            f_name = f.name
            self._fileMappingDict[f_name] = f

    def getFile(self, name: str):
        keys = self._fileMappingDict.keys()

        if name not in keys:
            raise FileNotFoundError(f"Not able to find given file name:{name}")
        else:
            return self._fileMappingDict[name]
