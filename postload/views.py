from django.http import HttpRequest
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.request import Request

from package import JoinProcess, ReadFields, ReadFile, Reconciliation

# Create your views here.


class ReconUser(APIView):
    def post(self, request: Request, format=None) -> Response:
        sourceFiles = ReadFile(request, "source")

        targetFiles = ReadFile(request, "target")

        sourceFields = ReadFields(request, "sourceFields")
        targetFields = ReadFields(request, "targetFields")

        sourceJoinsProcess = JoinProcess(request, "joins", "sourceJoins")
        sourceJoinsProcess.perform_operation(sourceFiles, sourceFields)
        sourceResult = sourceJoinsProcess.get_join_result()

        targetJoinsProcess = JoinProcess(request, "joins", "targetJoins")
        targetJoinsProcess.perform_operation(targetFiles, targetFields)
        targetResult = targetJoinsProcess.get_join_result()

        recon = Reconciliation(request, "reconJoin")
        s_vs_t, t_vs_s = recon.postload(sourceResult, targetResult)

        kpis = recon.kpis()

        return Response(
            {"source vs target": s_vs_t, "target vs source": t_vs_s, "kpis": kpis}
        )
