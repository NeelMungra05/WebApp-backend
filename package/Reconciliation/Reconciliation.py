from typing import List, Literal, Tuple

import numpy as np
import pandas as pd
from rest_framework.request import Request

from package import ReqToDict


class Reconciliation(ReqToDict):
    __summary_fields: list[str] = [
        "Fields",
        "Total Count",
        "Match Count",
        "Mismatch Count",
    ]

    def __init__(self, request: Request, attr: str) -> None:
        super().__init__(request, attr, "dict")
        self.src_to_trgt: pd.DataFrame
        self.trgt_to_src: pd.DataFrame
        self.src_kds: list[str]
        self.trgt_kds: list[str]
        self.src_text: list[str]
        self.trgt_text: list[str]
        self.src_num: list[str]
        self.trgt_num: list[str]
        self.source_pk: list[str] = (
            self.result.get("sourcePK", []) if isinstance(self.result, dict) else []
        )
        self.target_pk: list[str] = (
            self.result.get("targetPK", []) if isinstance(self.result, dict) else []
        )
        self.source_order: list[str] = (
            self.result.get("sourceOrder", []) if isinstance(self.result, dict) else []
        )
        self.target_order: list[str] = (
            self.result.get("targetOrder", []) if isinstance(self.result, dict) else []
        )

    def __add_prefix_to_dataframe(self, df: pd.DataFrame, prefix: str) -> pd.DataFrame:
        return df.add_prefix(prefix)

    def __match_cols_for_recon(self, df: pd.DataFrame, source: bool) -> pd.DataFrame:
        order: list[str] = self.source_order if source else self.target_order
        return df[order]

    def __add_prefix_to_list(self, lst: list[str], prefix: str) -> list[str]:
        return [prefix + val for val in lst]

    def __add_source_target_prefix(
        self, src: pd.DataFrame, trgt: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        src = self.__add_prefix_to_dataframe(src, "Source_")
        trgt = self.__add_prefix_to_dataframe(trgt, "Target_")
        return src, trgt

    def __check_dataframe_is_unique(self, df: pd.DataFrame, pk: list[str]) -> bool:
        try:
            df.set_index(pk, verify_integrity=True)
        except ValueError as e:
            return False
        else:
            return True

    def __do_merging(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        left_on: list[str],
        right_on: list[str],
    ) -> pd.DataFrame:
        result_df = pd.merge(df1, df2, left_on=left_on, right_on=right_on, how="left")

        result_df.fillna("", inplace=True)

        print(result_df)

        return result_df

    def __create_match_cols(
        self,
        df: pd.DataFrame,
        src_cols: list[str],
        trgt_cols: list[str],
        match_cols: list[str],
    ) -> pd.DataFrame:
        for i, j, k in zip(match_cols, src_cols, trgt_cols):
            df[j] = df[j].astype(str).str.strip()
            df[k] = df[k].astype(str).str.strip()
            print(k, " ", j)
            df[i] = np.where(df[j] == df[k], "True", "False")

        return df

    def __rearrange_cols_in_dataframe(
        self,
        df: pd.DataFrame,
        src_cols: list[str],
        trgt_cols: list[str],
        match_cols: list[str],
    ) -> pd.DataFrame:
        final_order: List[str] = []

        for i, j, k in zip(src_cols, trgt_cols, match_cols):
            final_order.append(i)
            final_order.append(j)
            final_order.append(k)

        return df[final_order]

    def __take_kpis_cols(
        self, df: pd.DataFrame, isSource: bool, match_cols: list[str], cols: list[str]
    ) -> None:
        kds_col: List[str] = []
        text_col: List[str] = []
        num_col: List[str] = []

        for col, match_col in zip(cols, match_cols):
            dtype = df[col].dtype
            if dtype == str or dtype == object:
                df[col] = df[col].fillna("")
                max_size: int = df[col].map(len).max()
                kds_col.append(match_col) if max_size <= 20 else text_col.append(
                    match_col
                )
            else:
                num_col.append(match_col)

        if isSource:
            self.src_kds = kds_col
            self.src_text = text_col
            self.src_num = num_col
        else:
            self.trgt_kds = kds_col
            self.trgt_text = text_col
            self.trgt_num = num_col

    def __create_summary(self, cols_name: list[str], df: pd.DataFrame) -> pd.DataFrame:
        summary: pd.DataFrame = pd.DataFrame(columns=self.__summary_fields)

        summary["Fields"] = cols_name
        summary["Match Count"] = [df[field].eq("True").sum() for field in cols_name]
        summary["Mismatch Count"] = [df[field].eq("False").sum() for field in cols_name]
        summary["Total Count"] = summary["Match Count"] + summary["Mismatch Count"]

        return summary

    def postload(
        self, source: pd.DataFrame, target: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if not self.__check_dataframe_is_unique(
            source, self.source_pk
        ) and not self.__check_dataframe_is_unique(target, self.target_pk):
            raise Exception("Primary key is not unique")

        source = self.__match_cols_for_recon(source, True)
        target = self.__match_cols_for_recon(target, False)

        match_col_source = self.__add_prefix_to_list(source.columns.to_list(), "Match_")
        match_col_target = self.__add_prefix_to_list(target.columns.tolist(), "Match_")

        source, target = self.__add_source_target_prefix(source, target)

        source_cols: list[str] = source.columns.tolist()
        target_cols: list[str] = target.columns.to_list()

        self.__take_kpis_cols(source, True, match_col_source, source_cols)
        self.__take_kpis_cols(target, False, match_col_target, target_cols)

        prfx_source_pk = self.__add_prefix_to_list(self.source_pk, "Source_")
        prfx_target_pk = self.__add_prefix_to_list(self.target_pk, "Target_")

        source = source.astype(str)
        target = target.astype(str)

        recon_src_to_trgt = self.__do_merging(
            source, target, prfx_source_pk, prfx_target_pk
        )
        recon_trgt_to_src = self.__do_merging(
            target, source, prfx_target_pk, prfx_source_pk
        )

        recon_src_to_trgt = self.__create_match_cols(
            recon_src_to_trgt, source_cols, target_cols, match_col_source
        )
        recon_trgt_to_src = self.__create_match_cols(
            recon_trgt_to_src, source_cols, target_cols, match_col_target
        )

        recon_src_to_trgt = self.__rearrange_cols_in_dataframe(
            recon_src_to_trgt, source_cols, target_cols, match_col_source
        )
        recon_trgt_to_src = self.__rearrange_cols_in_dataframe(
            recon_trgt_to_src, source_cols, target_cols, match_col_target
        )

        summary_src_to_trgt = self.__create_summary(match_col_source, recon_src_to_trgt)
        summary_trgt_to_src = self.__create_summary(match_col_target, recon_trgt_to_src)

        self.src_to_trgt = summary_src_to_trgt
        self.trgt_to_src = summary_trgt_to_src

        return summary_src_to_trgt, summary_trgt_to_src

    def kpis(self) -> dict[str, list[float]]:
        src_trgt_total_true_cnt: int = self.src_to_trgt["Match Count"].sum(axis=0)
        src_trgt_total_cnt: int = self.src_to_trgt["Total Count"].sum(axis=0)

        trgt_src_total_true_cnt: int = self.trgt_to_src["Match Count"].sum(axis=0)
        trgt_src_total_cnt: int = self.trgt_to_src["Total Count"].sum(axis=0)

        src_trgt_kds_true_cnt: int = self.src_to_trgt.loc[
            self.src_to_trgt["Fields"].isin(self.src_kds)
        ]["Match Count"].sum(axis=0)
        trgt_src_kds_true_cnt: int = self.trgt_to_src.loc[
            self.trgt_to_src["Fields"].isin(self.trgt_kds)
        ]["Match Count"].sum(axis=0)

        src_trgt_text_true_cnt: int = self.src_to_trgt.loc[
            self.src_to_trgt["Fields"].isin(self.src_text)
        ]["Match Count"].sum(axis=0)
        trgt_src_text_true_cnt: int = self.trgt_to_src.loc[
            self.trgt_to_src["Fields"].isin(self.trgt_text)
        ]["Match Count"].sum(axis=0)

        src_trgt_num_true_cnt: int = self.src_to_trgt.loc[
            self.src_to_trgt["Fields"].isin(self.src_num)
        ]["Match Count"].sum(axis=0)
        trgt_src_num_true_cnt: int = self.trgt_to_src.loc[
            self.trgt_to_src["Fields"].isin(self.trgt_num)
        ]["Match Count"].sum(axis=0)

        src_trgt_recon_prct: float = (
            src_trgt_total_true_cnt / src_trgt_total_cnt
        ) * 100
        trgt_src_recon_prct: float = (
            trgt_src_total_true_cnt / trgt_src_total_cnt
        ) * 100

        src_trgt_kds_prct: float = (src_trgt_kds_true_cnt / src_trgt_total_cnt) * 100
        trgt_src_kds_prct: float = (trgt_src_kds_true_cnt / trgt_src_total_cnt) * 100

        src_trgt_text_prct: float = (src_trgt_text_true_cnt / src_trgt_total_cnt) * 100
        trgt_src_text_prct: float = (trgt_src_text_true_cnt / trgt_src_total_cnt) * 100

        src_trgt_num_prct: float = (src_trgt_num_true_cnt / src_trgt_total_cnt) * 100
        trgt_src_num_prct: float = (trgt_src_num_true_cnt / trgt_src_total_cnt) * 100

        return {
            "src_trgt": [
                src_trgt_recon_prct,
                src_trgt_kds_prct,
                src_trgt_text_prct,
                src_trgt_num_prct,
            ],
            "trgt_src": [
                trgt_src_recon_prct,
                trgt_src_kds_prct,
                trgt_src_text_prct,
                trgt_src_num_prct,
            ],
        }
