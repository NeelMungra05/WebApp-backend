from django.http import HttpRequest
from rest_framework.response import Response
from rest_framework.views import APIView

from package import JoinProcess, ReadFields, ReadFile, Reconciliation

# Create your views here.


class ReconUser(APIView):
    def post(self, request: HttpRequest, format=None) -> Response:
        sourceFiles = ReadFile(request.FILES.getlist("source"))

        targetFiles = ReadFile(request.FILES.getlist("target"))

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

        src_kpis_list: list[str] = recon.get_all_kpis("src")
        trgt_kpis_list: list[str] = recon.get_all_kpis("trgt")

        src_file = recon.get_files("src")
        trgt_file = recon.get_files("trgt")

        response: Response = Response(
            {
                "source vs target": s_vs_t,
                "target vs source": t_vs_s,
                "kpis": kpis,
                "src_kpis_list": src_kpis_list,
                "trgt_kpis_list": trgt_kpis_list,
                "src_file": src_file,
                "trgt_file": trgt_file,
            }
        )

        response["Content-Disposition"] = 'attachment; filename="data.csv"'

        return response
