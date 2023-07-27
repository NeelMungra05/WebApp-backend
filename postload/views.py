import json

import pandas as pd
from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.views import APIView

from package import CustomJoins, ReadFields, ReadFile

# Create your views here.


class ReconUser(APIView):
    def post(self, request, format=None):
        sourceFiles = ReadFile(
            inMemoryFileList=request.FILES.getlist('source'))

        sourceFields = ReadFields(request, "sourceFields")

        joins: dict = json.loads(request.POST['joins'])
        sourceJoins = joins['sourceJoins']

        joinResult: dict = {}

        for idx, join in enumerate(sourceJoins['joinType']):
            file_idx = idx + 1
            left_on = sourceJoins['joinOn'][idx]['leftOn']
            right_on = sourceJoins['joinOn'][idx]['rightOn']

            file1 = sourceJoins['fileOrder'][0]
            file2 = sourceJoins['fileOrder'][1]

            cj = CustomJoins(sourceFields)
            cj.read_two_table(sourceFiles.getFile(file1),
                              sourceFiles.getFile(file2))
            cj.do_joining(right_on=right_on, left_on=left_on, how=join)
            joinResult[join] = cj.get_joined_table()

        var = {}

        tmp = {}

        return Response({'data': var, "joins": joinResult})
