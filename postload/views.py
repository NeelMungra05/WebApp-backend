import json

import pandas as pd
from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.views import APIView

from package.ReadFileFromMemory.ReadFile import ReadFile

# Create your views here.


class ReconUser(APIView):
    def post(self, request, format=None):
        targetFiles = request.FILES.getlist('target')

        sourceFiles = ReadFile(
            inMemoryFileList=request.FILES.getlist('source'))

        sourceFields: dict = json.loads(request.data['sourceFields'])
        joins: dict = json.loads(request.POST['joins'])
        sourceJoins = joins['sourceJoins']

        joinResult: dict = {}

        for idx, join in enumerate(sourceJoins['joinType']):
            file_idx = idx + 1
            left_on = sourceJoins['joinOn'][idx]['leftOn']
            right_on = sourceJoins['joinOn'][idx]['rightOn']
            print(sourceJoins['fileOrder'][0])
            file1 = pd.read_excel(sourceFiles.getFile(
                sourceJoins['fileOrder'][0]))
            file2 = pd.read_excel(sourceFiles.getFile(
                sourceJoins['fileOrder'][1]))

            result = pd.merge(file1, file2, left_on=left_on,
                              right_on=right_on, how=join)
            joinResult['result'] = result

        var = {}

        tmp = {}

        return Response({'data': var, "joins": joinResult})
