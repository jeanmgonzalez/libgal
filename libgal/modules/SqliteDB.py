from abc import abstractmethod
from typing import List, Optional
import pandas as pd
import sqlalchemy
from pandas import DataFrame
from libgal.modules.DatabaseAPI import DatabaseAPI, FunctionNotImplementedException
from libgal.modules.Logger import Logger
import re
from libgal.modules.ODBCTools import load_table, load_sql
from libgal.modules.Utils import drop_lists, chunks_df

logger = Logger(dirname=None).get_logger()


class SqliteDB(DatabaseAPI):

    def __init__(self, dbfile, drop_tables=False):
        self.filepath = dbfile
        self.conn, self.eng = self.connect()
        self.should_drop_tables = drop_tables

    def connect(self):
        eng = sqlalchemy.create_engine(f'sqlite:///{self.filepath}')
        conn = eng.raw_connection()
        return conn, eng

    def do(self, query: str):
        c = self.conn.cursor()
        logger.debug(f'Ejecutando query: {query}')
        c.execute(query)
        c.close()
        self.conn.commit()

    def read_table(self, table: str):
        return load_table(self.engine, table)

    def insert(self, df: DataFrame, schema: Optional[str], table: str, pk: str,
               odbc_limit: int = 100000):
        parts = chunks_df(df, odbc_limit)
        total = len(parts)
        for i, chunk in enumerate(parts):
            logger.info(f'Cargando lote {i+1} de {total}')
            chunk.to_sql(name=table, con=self.engine, schema=schema, if_exists='append', index=False)

    def upsert(self, df: DataFrame, schema: Optional[str], table: str, pk: str):
        self.delete_by_primary_key(df, schema, table, pk)
        self.insert(df, schema, table, pk)

    def diff(self, schema_src: Optional[str], table_src: str, schema_dst: Optional[str], table_dst: str) -> DataFrame:
        if schema_src is not None and schema_dst is not None:
            query = f'SELECT * FROM "{schema_src}.{table_src}" EXCEPT SELECT * FROM "{schema_dst}.{table_dst}";'
        elif schema_src is not None:
            query = f'SELECT * FROM "{schema_src}.{table_src}" EXCEPT SELECT * FROM "{table_dst}";'
        elif schema_dst is not None:
            query = f'SELECT * FROM "{table_src}" EXCEPT SELECT * FROM "{schema_dst}.{table_dst}";'
        else:
            query = f'SELECT * FROM "{table_src}" EXCEPT SELECT * FROM "{table_dst}";'

        difference = self.query(query)
        return difference

    def staging_insert(self, df: DataFrame, schema_src: Optional[str], table_src: str,
                       schema_dst: Optional[str], table_dst: str, pk: str):
        raise FunctionNotImplementedException('staging_insert no está implementado para SQLite')

    def staging_upsert(self, df: DataFrame, schema_src: Optional[str], table_src: str, schema_dst: Optional[str],
                       table_dst: str, pk: str):
        raise FunctionNotImplementedException('staging_upsert no está implementado para SQLite')

    def load_sql(self, path: str):
        return load_sql(path)

    def query(self, query: str):
        return pd.read_sql(sql=query, con=self.engine, index_col=None, coerce_float=True,
                           parse_dates=None, columns=None, chunksize=None)

    def table_columns(self, schema: Optional[str], table: str) -> List[str]:
        query = f'SELECT * FROM "{schema}.{table}" LIMIT 1;'
        result = self.query(query)
        return result.columns.tolist()

    def truncate_table(self, schema: Optional[str], table: str):
        if schema is not None:
            query = f'DELETE FROM "{schema}.{table}";'
        else:
            query = f'DELETE FROM "{table}";'
        self.do(query)

    @staticmethod
    def cursor_execute(c, query_split):
        for query in query_split:
            if len(query) > 0:
                c.execute(query + ';')

    def drop_table(self, schema: Optional[str], table: str):
        if schema is not None:
            query = f'DROP TABLE IF EXISTS "{schema}.{table}";'
        else:
            query = f'DROP TABLE IF EXISTS "{table}";'
        self.do(query)

    def create_table_like(self, schema: Optional[str], table: str, schema_orig: Optional[str], table_orig: str):
        if schema_orig is not None and schema is not None:
            query = f'CREATE TABLE "{schema}.{table}" AS SELECT * FROM "{schema_orig}.{table_orig}" LIMIT 1;'
        elif schema_orig is not None:
            query = f'CREATE TABLE "{table}" AS SELECT * FROM "{schema_orig}.{table_orig}" LIMIT 1;'
        elif schema is not None:
            query = f'CREATE TABLE "{schema}.{table}" AS SELECT * FROM "{table_orig}" LIMIT 1;'
        else:
            query = f'CREATE TABLE "{table}" AS SELECT * FROM "{table_orig}" LIMIT 1;'

        self.do(query)
        self.truncate_table(schema, table)

    def drop_tables(self, tables: List[str]):
        for table in tables:
            query = f'DROP TABLE IF EXISTS "{table}";'
            self.do(query)

    def drop_views(self, views: List[str]):
        for view in views:
            query = f'DROP VIEW IF EXISTS "{view}";'
            self.do(query)

    def strip_names(self, df: DataFrame, name: str):
        col_names = df.columns
        new_names = [re.sub(name, '', col) for col in col_names]
        dict_names = dict(zip(col_names, new_names))
        renamed = df.rename(columns=dict_names)
        return renamed, col_names

    def select_path(self, df: DataFrame, pattern):
        to_strip = df.iloc[:, df.columns.str.contains(f'^{pattern}\..*')]
        return self.strip_names(to_strip, f'^{pattern}\.')

    def select_cols(self, df: DataFrame, pattern):
        to_strip = df.iloc[:, df.columns.str.contains(f'^{pattern}.*')]
        return self.strip_names(to_strip, f'^{pattern}')

    def key_exists(self, table: str, field_id: str, key):
        query = f'SELECT * FROM {table} WHERE {field_id}="{key}";'
        result_df = self.query(query)
        return not result_df.empty

    @staticmethod
    def drop_lists(df: DataFrame):
        return drop_lists(df)

    @property
    def connection(self):
        return self.conn

    @property
    def engine(self):
        return self.eng

