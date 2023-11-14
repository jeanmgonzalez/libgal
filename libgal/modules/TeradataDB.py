import datetime
import math
from typing import Optional, List

import pyodbc
import pandas as pd
import teradatasql
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from libgal.modules.DatabaseAPI import DatabaseAPI, DatabaseError
from libgal.modules.Logger import Logger
from libgal.modules.ODBCTools import load_table, inserts_from_dataframe
from time import sleep
from teradataml.context.context import create_context
from teradataml.dataframe.fastload import fastload
from teradatasql import OperationalError as tdOperationalError
from libgal.modules.Utils import chunks_df

logger = Logger(dirname=None).get_logger()


class Scripting:

    def __init__(self):
        self._script = []

    def begin_transaction(self):
        self._script.append({
            'statement': 'BEGIN TRANSACTION',
            'values': []
        })

    def end_transaction(self):
        self._script.append({
            'statement': 'END TRANSACTION;',
            'values': []
        })

    def insert_batch(self, df, schema, table):
        columns = str(', '.join(df.columns))
        for index, row in df.iterrows():
            try:
                for name, value in row.items():
                    if isinstance(value, datetime.date):
                        row[name] = value.strftime('%Y-%m-%d')
                    elif isinstance(value, datetime.time):
                        row[name] = value.strftime('%H:%M:%S')
                    elif value is None or (isinstance(value, float) and math.isnan(value)):
                        row[name] = None
                    elif isinstance(value, str):
                        row[name] = value.replace("'", '').strip()
                    elif ('Id' in name or 'Num' in name) and value == int(value):
                        row[name] = int(value)
            except ValueError as e:
                logger.error(e)
                logger.debug(row)
                raise e

            insert = f'INSERT INTO {schema}.{table}' + ' (' + columns + ') VALUES (' + \
                     ','.join(['?'] * len(row.values)) + ');'
            self._script.append({
                'statement': insert,
                'values': row.values
            })

    def delete_by_index(self, df, schema, table, pk):
        values = df[pk].unique()
        statement = f'DELETE FROM {schema}.{table} WHERE {pk} IN ({", ".join(self._stringify(values))});'
        self._script.append(
            {
                'statement': statement,
                'values': []
            }
        )

    def delete_by_table(self, schema, table, stg_schema, stg_table, pk):
        statement = f'DELETE FROM {schema}.{table} WHERE {pk} IN (SEL {pk} FROM {stg_schema}.{stg_table});'
        self._script.append(
            {
                'statement': statement,
                'values': []
            }
        )

    def insert_from_table(self, schema_orig, table_orig, schema_dest, table_dest):
        statement = f'INSERT INTO {schema_dest}.{table_dest} SELECT * FROM {schema_orig}.{table_orig};'
        self._script.append(
            {
                'statement': statement,
                'values': []
            }
        )

    def drop_table(self, schema, table):
        statement = f'DROP TABLE {schema}.{table};'
        self._script.append(
            {
                'statement': statement,
                'values': []
            }
        )

    def add_statement(self, statement):
        self._script.append(
            {
                'statement': statement,
                'values': []
            }
        )

    @property
    def statements(self):
        return self._script

    def _stringify(self, values):
        return [f"'{x}'" if isinstance(x, str) else str(x) for x in values]

    @property
    def script(self):
        str_arr = []
        import re
        for item in self._script:
            if len(item['values']) > 0:
                statement = re.sub(r'VALUES \(.*\?.*\);', '', item['statement'])
                str_arr.append(statement + 'VALUES (' + ', '.join(self._stringify(item['values'])) + ');')
            else:
                str_arr.append(item['statement'])
        return str_arr


class TeradataDB(DatabaseAPI):

    def __init__(self, host: str, user: str, passw: str,
                 logmech: Optional[str] = None, schema: Optional[str] = 'DBC'):

        self.context: Optional[Engine] = None
        self.eng: Optional[Engine] = None
        self.conn = None
        self.schema = schema
        self.logmech = logmech
        self._conn_params = {
            'host': host,
            'user': user,
            'pass': passw,
        }
        self.connect()

    def connect(self):
        user, passw, host, schema = self._conn_params['user'], self._conn_params['pass'], \
                                    self._conn_params['host'], self.schema

        if self.schema is not None:
            logger.info('Conectando TeradataML')
            self.tml_connect(host, user, passw, schema, self.logmech)
            self.conn = self.context.raw_connection()
            self.eng: Engine = self.context
        else:
            raise DatabaseError(
                'Fastload solo se puede usar si se inicializa Teradata especificando un schema'
            )

    def use_db(self, db: str):
        self.do(f'DATABASE {db};')

    def do(self, query: str):
        c = self.conn.cursor()
        if isinstance(query, list):
            query_len = len(query)
            lock_echo = 0
            logger.info(f'Tamaño de la query: {query_len} sentencias')
            for ix, item in enumerate(query):
                percent = ix * 100 / query_len
                if int(percent) % 2 == 0 and int(percent) != lock_echo:
                    logger.info(f'Ejecutando SQL script, {int(percent)}% completado')
                    lock_echo = int(percent)
                try:
                    if len(item['values']) > 0:
                        c.execute(item['statement'], *list(item['values']))
                    else:
                        c.execute(item['statement'])
                except (pyodbc.ProgrammingError, pyodbc.Error, pyodbc.IntegrityError, UnicodeEncodeError) as e:
                    logger.error(str(e).replace('\\x00', ''))
                    logger.debug(item['statement'])
                    if len(item['values']) > 0:
                        logger.debug(item['values'])
                    raise DatabaseError

        else:
            logger.debug(f'Ejecutando query: {query}')
            c.execute(query)

        c.close()
        self.conn.commit()

    def query(self, query: str, mode: str = 'normal') -> DataFrame:
        logger.debug(f'Ejecutando query: {query}')
        if mode == 'normal':
            return pd.read_sql(query, self.engine)
        else:
            return pd.read_sql(query, self.connection)

    def current_date(self) -> datetime.date:
        query = "select current_date;"
        result_df = self.query(query)
        return result_df['Date'][0]

    def show_tables(self, db: str, prefix: str) -> DataFrame:
        query = f"""SELECT  DatabaseName,
            TableName,
            CreateTimeStamp,
            LastAlterTimeStamp
        FROM    DBC.TablesV
        WHERE   TableKind = 'T'
        AND lower(DatabaseName) = '{db.lower()}'
        AND TableName LIKE '{prefix.lower()}%'
        ORDER BY    TableName;
        """
        return self.query(query)

    def drop_table(self, schema: str, table: str):
        query = f'DROP TABLE {schema}.{table};'
        self.do(query)

    def truncate_table(self, schema: str, table: str):
        query = f'DELETE FROM {schema}.{table} ALL;'
        self.do(query)

    def table_columns(self, schema: str, table: str) -> List[str]:
        query = f'SEL TOP 1 * FROM {schema}.{table};'
        result = self.query(query)
        return result.columns.tolist()

    def create_table_like(self, schema: str, table: str, schema_orig: str, table_orig: str):
        query = f'CREATE TABLE {schema}.{table} AS {schema_orig}.{table_orig} WITH NO DATA;'
        self.do(query)

    def insert(self, df: DataFrame, schema: str, table: str, pk: str,
               use_odbc: bool = True, odbc_limit: int = 10000):
        if len(df) <= odbc_limit and use_odbc:
            parts = chunks_df(df, 5000)
            total = len(parts)
            for i, chunk in enumerate(parts):
                logger.info(f'Cargando lote {i+1} de {total}')
                chunk.to_sql(name=table, con=self.engine, schema=schema, if_exists='append', index=False)
        else:
            self.retry_fastload(df, schema, table, pk)

    def upsert(self, df: DataFrame, schema: str, table: str, pk: str,
               use_odbc: bool = True, odbc_limit: int = 10000, parser_limit: int = 10000):
        self.delete_by_primary_key(df, schema, table, pk, parser_limit)
        self.insert(df, schema, table, pk, use_odbc, odbc_limit)

    def get_inserts_from_table(self, schema: str, table: str):
        tablename = f'{schema}.{table}'
        df = load_table(self.engine, tablename, use_quotes=False)
        return inserts_from_dataframe(df, tablename)

    @property
    def connection(self):
        return self.conn

    @property
    def engine(self):
        return self.eng

    def tml_connect(self, host, user, passw, db, logmech=None):
        if logmech is None:
            context = create_context(host=host, user=user, password=passw, database=db)
        else:
            context = create_context(host=host, user=user, password=passw, logmech=logmech, database=db)

        self.context = context
        return context

    def fastload(self, df, schema, table, pk, index=False):
        fastload(df, schema_name=schema, table_name=table, primary_index=pk, index=index)

    def retry_fastload(self, df, schema, table, pk, retries=30, retry_sleep=20):
        size = len(df)
        while retries > 0:
            try:
                logger.info(f'Ejecutando fastload en {schema}.{table} ({size} filas)')
                self.fastload(df, schema=schema, table=table, pk=pk, index=False)
                return True
            except tdOperationalError as e:
                if '2663' in e:
                    logger.warning(e)
                    sleep(retry_sleep)
                    retries -= 1
                else:
                    raise e
        raise DatabaseError('Se superaron todos los reintentos de fastload')

    def diff(self, schema_src: str, table_src: str, schema_dst: str, table_dst: str) -> DataFrame:
        query = f"SELECT * FROM {schema_src}.{table_src} MINUS SELECT * FROM {schema_dst}.{table_dst};"
        difference = self.query(query)
        return difference

    def _get_named_cols(self, schema_stg: str, table_stg: str, schema_dst: str, table_dst: str, prefix: str):
        try:
            columns = self.table_columns(schema_dst, table_dst)
        except (pyodbc.ProgrammingError, teradatasql.OperationalError, OperationalError):
            logger.warning('La tabla destino no existe, creando DDL por inferencia de tipos de datos')
            columns = self.table_columns(schema_stg, table_stg)
            self.create_table_like(schema_dst, table_dst, schema_stg, table_stg)

        return [f'{prefix}.{x}' for x in columns]

    def forced_drop_table(self, schema: str, table: str):
        try:
            self.drop_table(schema, table)
        except (pyodbc.ProgrammingError, teradatasql.OperationalError):
            pass

    def staging_insert(self, df: DataFrame, schema_stg: str, table_stg: str, schema_dst: str, table_dst: str, pk: str):
        self.forced_drop_table(schema_stg, table_stg)
        self.retry_fastload(df, schema_stg, table_stg, pk)

        named_columns = self._get_named_cols(schema_stg, table_stg, schema_dst, table_dst, 'stg')
        query = f'INSERT INTO {schema_dst}.{table_dst} SELECT {", ".join(named_columns)} ' + \
            f'FROM {schema_stg}.{table_stg} stg ' + \
            f'LEFT JOIN {schema_dst}.{table_dst} prd ON prd.{pk} = stg.{pk} ' + \
            f'WHERE prd.{pk} IS NULL;'

        self.do(query)
        self.drop_table(schema_stg, table_stg)

    def staging_upsert(self, df: DataFrame, schema_stg: str, table_stg: str, schema_dst: str,
                       table_dst: str, pk: str, parser_limit: int = 10000):
        self.forced_drop_table(schema_stg, table_stg)
        self.retry_fastload(df, schema_stg, table_stg, pk)
        self.delete_by_primary_key(df, schema_dst, table_dst, pk, parser_limit)

        named_columns = self._get_named_cols(schema_stg, table_stg, schema_dst, table_dst, 'stg')
        query = f'INSERT INTO {schema_dst}.{table_dst} SELECT {", ".join(named_columns)} ' + \
            f'FROM {schema_stg}.{table_stg} stg ' + \
            f'LEFT JOIN {schema_dst}.{table_dst} prd ON prd.{pk} = stg.{pk} ' + \
            f'WHERE prd.{pk} IS NULL;'

        self.do(query)
        self.drop_table(schema_stg, table_stg)
