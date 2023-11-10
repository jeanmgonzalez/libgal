from abc import ABC, abstractmethod
from typing import List, Optional

import pandas as pd
from pandas import DataFrame
from libgal.modules.Utils import chunks


class FunctionNotImplementedException(Exception):
    pass


class DatabaseAPI(ABC):

    @abstractmethod
    def connect(self):
        ...

    @abstractmethod
    def do(self, query: str):
        ...

    def execute(self, query: str):
        self.do(query)

    @abstractmethod
    def query(self, query: str) -> DataFrame:
        ...

    @abstractmethod
    def drop_table(self, schema: Optional[str], table: str):
        ...

    @abstractmethod
    def truncate_table(self, schema: Optional[str], table: str):
        ...

    @abstractmethod
    def table_columns(self, schema: Optional[str], table: str) -> List[str]:
        ...

    @staticmethod
    def _pks_as_in_statement(pks):
        if pd.api.types.is_numeric_dtype(pks):
            pk_in_list = ','.join(pks.unique().astype(dtype=str).tolist())
        else:
            pk_in_list = "'" + "','".join(pks.unique().astype(dtype=str).tolist()) + "'"
        return pk_in_list

    def delete_by_primary_key(self, df: DataFrame, schema: Optional[str], table: str, pk: str, parser_limit=10000):
        if not df.empty:
            for pks in chunks(df[pk], parser_limit):
                in_list = self._pks_as_in_statement(pks)
                if schema is not None:
                    escaped_table_name = f'{schema}.{table}'
                else:
                    escaped_table_name = f'{table}' if '.' not in table else f'"{table}"'
                query = f"DELETE FROM {escaped_table_name} WHERE {pk} IN ({in_list});"
                self.do(query)

    @abstractmethod
    def insert(self, df: DataFrame, schema: Optional[str], table: str, pk: str):
        ...

    @abstractmethod
    def upsert(self, df: DataFrame, schema: Optional[str], table: str, pk: str):
        ...

    def insert_overwrite(self, df: DataFrame, schema: Optional[str], table: str, pk: str):
        self.upsert(df, schema, table, pk)

    @abstractmethod
    def diff(self, schema_src: Optional[str], table_src: str,
             schema_dst: Optional[str], table_dst: str) -> DataFrame:
        ...

    @abstractmethod
    def staging_insert(self, df: DataFrame, schema_src: Optional[str], table_src: str,
                       schema_dst: Optional[str], table_dst: str, pk: str):
        ...

    @abstractmethod
    def staging_upsert(self, df: DataFrame, schema_src: Optional[str], table_src: str,
                       schema_dst: Optional[str], table_dst: str, pk: str):
        ...
