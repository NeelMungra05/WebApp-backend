import json

import pandas as pd
from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.views import APIView

# Create your views here.


class ReconUser(APIView):
    def post(self, request, format=None):
        sourceFiles = request.FILES.getlist('source')
        targetFiles = request.FILES.getlist('target')

        sourceFields: dict = json.loads(request.data['sourceFields'])
        joins: dict = json.loads(request.POST['joins'])
        sourceJoins = joins['sourceJoins']

        joinResult: dict = {}

        for idx, join in enumerate(sourceJoins['joinType']):
            file_idx = idx + 1
            left_on = sourceJoins['joinOn'][idx]['leftOn']
            right_on = sourceJoins['joinOn'][idx]['rightOn']
            print(sourceJoins['fileOrder'][0])
            file1 = pd.read_excel(sourceFiles[0])
            file2 = pd.read_excel(sourceFiles[1])

            result = pd.merge(file1, file2, left_on=left_on,
                              right_on=right_on, how=join)
            joinResult['result'] = result

        var = {}

        tmp = {}
        for f in sourceFiles:
            f_name = f.name
            tmp[f_name] = f
            cols = list(sourceFields[f_name].keys())
            req_col = [x for x in cols if sourceFields[f_name][x]["RF"] == True]
            req_pk = [x for x in cols if sourceFields[f_name][x]["PK"] == True]

            var[f_name] = {'cols': req_col, 'req_pk': req_pk}

        print(tmp)

        return Response({'data': var, "joins": joinResult})
