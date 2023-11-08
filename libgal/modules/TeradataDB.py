import datetime
import math
from typing import Optional

import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from libgal.modules.Logger import Logger
from libgal.modules.ODBCTools import upsert_by_primary_key, load_table, inserts_from_dataframe
from time import sleep
from teradataml.context.context import create_context
from teradataml.dataframe.fastload import fastload
from teradatasql import OperationalError as tdOperationalError

logger = Logger.__call__().get_logger()


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


class DriverNotFoundException(Exception):
    "No se han encontrado drivers de ODBC disponibles"
    pass


class DatabaseError(Exception):
    "No se puede realizar la operación en la base de datos"
    pass


class TeradataDB:

    def __init__(self,
                 host: str, user: str, passw: str, db: Optional[str] = None,
                 logmech: Optional[str] = None, charset: str = 'iso-8859-15',
                 url_params: Optional[str] = None):
        self.charset = charset
        self.tml_connected = False
        self.context = None

        success, drivers = self.check_drivers()
        logger.info(f'Realizando conexión a la base de datos {host} con usuario {user}')
        td_drivers = [d for d in drivers if 'teradata' in d.lower()]
        if success and len(td_drivers) > 0:
            driver_name = td_drivers[0]
            logger.info(f'Utilizando driver: {driver_name}')
            if 'ldap' not in logmech.lower():
                self.odbclink = 'DRIVER={DRIVERNAME};DBCNAME={hostname};UID={uid};PWD={pwd}'.format(
                    DRIVERNAME=driver_name, hostname=host,
                    uid=user, pwd=passw)
            else:
                self.odbclink = 'AUTHENTICATION=LDAP;DRIVER={DRIVERNAME};DBCNAME={hostname};UID={uid};PWD={pwd}'.format(
                    DRIVERNAME=driver_name, hostname=host,
                    uid=user, pwd=passw)

            self.slalink = f'teradata:///?username={user}&password={passw}&host={host}?charset={self.charset}'
            self.conn, self.eng = self.connect()
            if db is not None:
                self.tml_connect(host, user, passw, db, logmech)

    def check_drivers(self):
        drivers = pyodbc.drivers()
        if len(drivers) > 0:
            logger.info(f'Drivers ODBC detectados: {drivers}')
            return True, drivers
        else:
            logger.error(f'No se han encontrado drivers ODBC instalados')
            return False, []

    def use_db(self, db):
        self.do(f'DATABASE {db};')

    def execute(self, query):
        return self.do(query)

    def do(self, query):
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
            logger.info(f'Ejecutando query: {query}')
            c.execute(query)

        c.commit()
        c.close()
        self.conn.commit()

    def connect_odbc(self):
        self.conn = pyodbc.connect(self.odbclink, autocommit=True)
        self.conn.setdecoding(pyodbc.SQL_CHAR, encoding=self.charset)
        self.conn.setdecoding(pyodbc.SQL_WCHAR, encoding=self.charset)
        self.conn.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-16le')
        self.conn.setencoding(encoding=self.charset)
        return self.conn

    def engine_connect(self):
        self.eng = create_engine(
            self.slalink,
            creator=self.connect_odbc,
            echo=False
        )
        return self.eng

    def connect(self):
        conn = self.connect_odbc()
        eng = self.engine_connect()
        return conn, eng

    def query(self, query, mode='normal'):
        logger.info(f'Ejecutando query: {query}')
        if mode == 'normal':
            return pd.read_sql(query, self.engine)
        else:
            return pd.read_sql(query, self.connection)

    def current_date(self) -> datetime.date:
        query = "select current_date;"
        result_df = self.query(query)
        return result_df['Current Date'][0]

    def show_tables(self, db, prefix):
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

    def _upsert_by_primary_key(self, schema, table, df, primary_key):
        df_from = pd.read_sql(f"SELECT {primary_key} FROM {schema}.{table};", self.eng)
        if df_from.empty:
            logger.debug(f'No hay registros insertados para el pk {primary_key} en {schema}.{table}')
            df.to_sql(table, self.eng, if_exists='append', index=False, schema=schema)
            df_result = df
        else:
            df_result = df[~df[primary_key].isin(df_from[primary_key])]
            if not df_result.empty:
                logger.info(f'{len(df_from)} registros ya insertados, insertando novedades ({len(df_result)} registros) en {schema}.{table}')
                df_result.to_sql(table, self.eng, if_exists='append', index=False, schema=schema)
            else:
                logger.info(f'{len(df_from)} registros ya insertados, no se realizan acciones')
        return df_result

    def _log_inserts(self, df_result, existing_records, primary_key, tablename):
        if len(df_result) == 0:
            logger.info(f'{len(existing_records)} registros existentes, no se realizan acciones')
        elif len(existing_records) == 0:
            logger.info(f'No existen registros para el pk {primary_key} en {tablename}')
        else:
            logger.info(f'{len(existing_records)} registros existentes, insertando novedades ({len(df_result)} registros) en {tablename}')

    def drop_table(self, schema, table):
        query = f'DROP TABLE {schema}.{table};'
        self.do(query)

    def create_table_like(self, schema, table, schema_orig, table_orig):
        query = f'CREATE TABLE {schema}.{table} AS {schema_orig}.{table_orig} WITH NO DATA;'
        self.do(query)

    def upsert_by_primary_key(self, schema, table, df, primary_key):
        df_result, existing_records = upsert_by_primary_key(self.eng, table, df, primary_key, schema=schema)
        self._log_inserts(df_result, existing_records, primary_key, table)
        return df_result

    def upsert_dataframe(self, schema, table, df, primary_key):
        df_result, existing_records = upsert_by_primary_key(self.eng, table, df, primary_key, schema=schema)
        self._log_inserts(df_result, existing_records, primary_key, table)

    def get_inserts_from_table(self, schema, table):
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
        self.tml_connected = True
        return context

    def fastload(self, df, schema, table, pk, index=False):
        if not self.tml_connected:
            raise DatabaseError(
                'Fastload solo se puede usar si se inicializa Teradata especificando un schema en el argumento db'
            )
        fastload(df, schema_name=schema, table_name=table, primary_index=pk, index=index)

    def retry_fastload(self, df, schema, table, pk, retries=30, retry_sleep=20):
        while retries > 0:
            try:
                self.fastload(df, schema=schema, table=table, pk=pk, index=False)
                break
            except tdOperationalError as e:
                if '2663' in e:
                    sleep(retry_sleep)
                    retries -= 1
                else:
                    raise e
