from typing import List, Literal, Tuple

import numpy as np
import pandas as pd
from django.http import HttpRequest

from package import ReqToDict


class Reconciliation(ReqToDict):

    __summary_fields: list[str] = [
        "Fields", "Total Count", "MatchCount", "Mismatch Count"]

    def __init__(self, request: HttpRequest, attr: str) -> None:
        super().__init__(request, attr)
        self.source_pk: list[str] = self.result.get('source', [])
        self.target_pk: list[str] = self.result.get('target', [])

    def __add_prefix_to_dataframe(self, df: pd.DataFrame, prefix: str) -> pd.DataFrame:
        return df.add_prefix(prefix)

    def __add_prefix_to_list(self, lst: list[str], prefix: str) -> list[str]:
        return [prefix+val for val in lst]

    def __add_source_target_prefix(self, src: pd.DataFrame, trgt: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
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

    def __do_merging(self, df1: pd.DataFrame, df2: pd.DataFrame, left_on: list[str], right_on: list[str]) -> pd.DataFrame:
        result_df = pd.merge(df1, df2, left_on=left_on,
                             right_on=right_on, how='left')

        result_df.fillna('', inplace=True)

        return result_df

    def __create_match_cols(self, df: pd.DataFrame, src_cols: list[str], trgt_cols: list[str], match_cols: list[str]) -> pd.DataFrame:
        for i, j, k in zip(match_cols, src_cols, trgt_cols):

            if df[j].dtype == str:
                df[j] = df[k].str.strip()

            if df[k].dtype == str:
                df[k] = df[k].str.strip()

            df[i] = np.where(df[j] == df[k], 'True', 'False')

        return df

    def __rearrange_cols_in_dataframe(self, df: pd.DataFrame, src_cols: list[str], trgt_cols: list[str], match_cols: list[str]) -> pd.DataFrame:

        final_order: List[str] = []

        for i, j, k in zip(src_cols, trgt_cols, match_cols):
            final_order.append(i)
            final_order.append(j)
            final_order.append(k)

        return df[final_order]

    def __create_summary(self, cols_name: list[str], df: pd.DataFrame) -> pd.DataFrame:
        summary: pd.DataFrame = pd.DataFrame(columns=self.__summary_fields)

        summary['Fields'] = cols_name
        summary['Match Count'] = df[cols_name].apply(
            lambda x: x.value_count().get('True', 0), axis=1)
        summary['Mismatch Count'] = df[cols_name].apply(
            lambda x: x.value_count().get('False', 0), axis=1)
        summary['Total Count'] = df.count()

        return summary

    def postload(self, source: pd.DataFrame, target: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if not self.__check_dataframe_is_unique(source, self.source_pk) and not self.__check_dataframe_is_unique(target, self.target_pk):
            raise Exception("Primary key is not unique")

        match_col_source = self.__add_prefix_to_list(
            source.columns.to_list(), "Match_")
        match_col_target = self.__add_prefix_to_list(
            target.columns.tolist(), "Match_")

        source, target = self.__add_source_target_prefix(source, target)

        source_cols: list[str] = source.columns.tolist()
        target_cols: list[str] = target.columns.to_list()

        prfx_source_pk = self.__add_prefix_to_list(self.source_pk, "Source_")
        prfx_target_pk = self.__add_prefix_to_list(self.target_pk, "Target_")

        recon_src_to_trgt = self.__do_merging(
            source, target, prfx_source_pk, prfx_target_pk)
        recon_trgt_to_src = self.__do_merging(
            target, source, prfx_target_pk, prfx_source_pk)

        recon_src_to_trgt = self.__create_match_cols(
            recon_src_to_trgt, source_cols, target_cols, match_col_source)
        recon_trgt_to_src = self.__create_match_cols(
            recon_trgt_to_src, source_cols, target_cols, match_col_target)

        recon_src_to_trgt = self.__rearrange_cols_in_dataframe(
            recon_src_to_trgt, source_cols, target_cols, match_col_source)
        recon_trgt_to_src = self.__rearrange_cols_in_dataframe(
            recon_trgt_to_src, source_cols, target_cols, match_col_target)

        summary_src_to_trgt = self.__create_summary(
            match_col_source, recon_src_to_trgt)
        summary_trgt_to_src = self.__create_summary(
            match_col_target, recon_trgt_to_src)

        return summary_src_to_trgt, summary_trgt_to_src
