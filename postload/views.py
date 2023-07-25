import pandas as pd
from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.views import APIView

# Create your views here.


class ReconUser(APIView):
    def post(self, request, format=None):
        fileName = request.FILES.getlist('source')

        var = {}

        for f in fileName:
            df = pd.DataFrame(f)
            var[f.name] = df.head()

        return Response({'data': var})
