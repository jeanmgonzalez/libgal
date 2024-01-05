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


class Sqlite(DatabaseAPI):

    def __init__(self, dbfile, drop_tables=False):
        """
        Crea una conexión a una base de datos SQLite
            :param dbfile: Ruta del archivo de la base de datos
            :param drop_tables: Si se debe borrar las tablas al crear la conexión
        """
        self.filepath = dbfile
        self.conn, self.eng = self.connect()
        self.should_drop_tables = drop_tables

    def connect(self):
        """
        Crea una conexión a la base de datos
            :return: Tupla con la conexión y el engine
        """
        eng = sqlalchemy.create_engine(f'sqlite:///{self.filepath}')
        conn = eng.raw_connection()
        return conn, eng

    def do(self, query: str):
        """
        Ejecuta una query que no devuelve resultados
            :param query: Query a ejecutar
        """
        c = self.conn.cursor()
        logger.debug(f'Ejecutando query: {query}')
        c.execute(query)
        c.close()
        self.conn.commit()

    def read_table(self, table: str) -> DataFrame:
        """
        Lee una tabla de la base de datos
            :param table: Nombre de la tabla a leer
            :return: DataFrame con el contenido de la tabla
        """
        return load_table(self.engine, table)

    def insert(self, df: DataFrame, schema: Optional[str], table: str, pk: str,
               odbc_limit: int = 100000):
        """
        Inserta un DataFrame en una tabla
            :param df: DataFrame a insertar
            :param schema: Esquema de la tabla
            :param table: Nombre de la tabla
            :param pk: Primary key de la tabla
            :param odbc_limit: Límite de filas por lote
        """
        parts = chunks_df(df, odbc_limit)
        total = len(parts)
        for i, chunk in enumerate(parts):
            logger.info(f'Cargando lote {i+1} de {total}')
            chunk.to_sql(name=table, con=self.engine, schema=schema, if_exists='append', index=False)

    def upsert(self, df: DataFrame, schema: Optional[str], table: str, pk: str):
        """
        Actualiza un DataFrame en una tabla
            :param df: DataFrame a actualizar
            :param schema: Esquema de la tabla
            :param table: Nombre de la tabla
            :param pk: Primary key de la tabla
        """
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
        """
        Carga un archivo SQL
            :param path: Ruta del archivo SQL
            :return: Contenido del archivo SQL
        """
        return load_sql(path)

    def query(self, query: str) -> DataFrame:
        """
        Ejecuta una query que devuelve resultados
            :param query: Query a ejecutar
            :return: DataFrame con el resultado de la query
        """
        return pd.read_sql(sql=query, con=self.engine, index_col=None, coerce_float=True,
                           parse_dates=None, columns=None, chunksize=None)

    def table_columns(self, schema: Optional[str], table: str) -> List[str]:
        """
        Devuelve las columnas de una tabla
            :param schema: Esquema de la tabla
            :param table: Nombre de la tabla
            :return: Lista de columnas de la tabla
        """
        query = f'SELECT * FROM "{schema}.{table}" LIMIT 1;'
        result = self.query(query)
        return result.columns.tolist()

    def truncate_table(self, schema: Optional[str], table: str):
        """
        Trunca una tabla (borra todos los registros pero no la estructura)
        """
        if schema is not None:
            query = f'DELETE FROM "{schema}.{table}";'
        else:
            query = f'DELETE FROM "{table}";'
        self.do(query)

    @staticmethod
    def cursor_execute(c, query_split):
        """
        Ejecuta una query que no devuelve resultados
            :param c: Cursor
            :param query_split: Query a ejecutar
        """
        for query in query_split:
            if len(query) > 0:
                c.execute(query + ';')

    def drop_table(self, schema: Optional[str], table: str):
        """
        Borra una tabla
            :param schema: Esquema de la tabla
            :param table: Nombre de la tabla
        """
        if schema is not None:
            query = f'DROP TABLE IF EXISTS "{schema}.{table}";'
        else:
            query = f'DROP TABLE IF EXISTS "{table}";'
        self.do(query)

    def create_table_like(self, schema: Optional[str], table: str, schema_orig: Optional[str], table_orig: str):
        """
        Crea una tabla con la misma estructura que otra
            :param schema: Schema de la tabla a crear
            :param table: Nombre de la tabla a crear
            :param schema_orig: Schema de la tabla original
            :param table_orig: Nombre de la tabla original
        """
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
        """
        Borra una lista de tablas
            :param tables: Lista de tablas a borrar
        """
        for table in tables:
            query = f'DROP TABLE IF EXISTS "{table}";'
            self.do(query)

    def drop_views(self, views: List[str]):
        """
        Borra una lista de vistas
            :param views: Lista de vistas a borrar
        """
        for view in views:
            query = f'DROP VIEW IF EXISTS "{view}";'
            self.do(query)

    def strip_names(self, df: DataFrame, name: str):
        """
        Elimina un prefijo de los nombres de las columnas
            :param df: DataFrame a renombrar
            :param name: Prefijo a eliminar
            :return: DataFrame con los nombres de las columnas renombrados
        """
        col_names = df.columns
        new_names = [re.sub(name, '', col) for col in col_names]
        dict_names = dict(zip(col_names, new_names))
        renamed = df.rename(columns=dict_names)
        return renamed, col_names

    def select_path(self, df: DataFrame, pattern: str):
        """
        Selecciona las columnas de un DataFrame que coincidan con un patrón
            :param df: DataFrame a filtrar
            :param pattern: Patrón a buscar
            :return: DataFrame con las columnas filtradas
        """
        to_strip = df.iloc[:, df.columns.str.contains(f'^{pattern}\..*')]
        return self.strip_names(to_strip, f'^{pattern}\.')

    def select_cols(self, df: DataFrame, pattern: str):
        """
        Selecciona las columnas de un DataFrame que coincidan con un patrón
            :param df: DataFrame a filtrar
            :param pattern: Patrón a buscar
            :return: DataFrame con las columnas filtradas
        """
        to_strip = df.iloc[:, df.columns.str.contains(f'^{pattern}.*')]
        return self.strip_names(to_strip, f'^{pattern}')

    def key_exists(self, table: str, field_id: str, key) -> bool:
        """
        Verifica si existe una clave en una tabla
            :param table: Nombre de la tabla
            :param field_id: Campo de la clave
            :param key: Valor de la clave
            :return: True si existe, False si no
        """
        query = f'SELECT * FROM {table} WHERE {field_id}="{key}";'
        result_df = self.query(query)
        return not result_df.empty

    @staticmethod
    def drop_lists(df: DataFrame):
        """
        Elimina las columnas que contengan listas
            :param df: DataFrame a limpiar
            :return: DataFrame sin columnas que contengan listas
        """
        return drop_lists(df)

    @property
    def connection(self):
        """
        Devuelve la conexión ODBC a la base de datos
        """
        return self.conn

    @property
    def engine(self):
        """
        Devuelve el engine de SQLAlchemy
        """
        return self.eng

