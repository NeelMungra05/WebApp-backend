
from rest_framework.response import Response
from rest_framework.views import APIView

from package import JoinProcess, ReadFields, ReadFile

# Create your views here.


class ReconUser(APIView):
    def post(self, request, format=None):
        sourceFiles = ReadFile(
            inMemoryFileList=request.FILES.getlist('source'))

        sourceFields = ReadFields(request, "sourceFields")

        joinsProcess = JoinProcess(request, "joins", 'sourceJoins')
        joinsProcess.perform_operation(sourceFiles, sourceFields)
        result = joinsProcess.get_join_result()

        return Response({"result": result})
