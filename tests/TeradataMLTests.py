import unittest
from time import time
import pyodbc
from pandas import DataFrame
from test_dataframe import generate_dataframe
from libgal.modules.TeradataDB import TeradataDB
from libgal.modules.Logger import Logger

logger = Logger().get_logger()


def ask_user_pwd():
    host = input(f'Ingrese host a conectarse: ')
    logmech = 'LDAP' if input(f'Debería usar LDAP para autenticar (s/n)?: ').strip().lower() == 's' else None
    db = input(f'Ingrese schema default de sesión (ej, p_staging): ')
    usr = input(f'Ingrese usuario de conexión: ')
    passw = input(f'Ingrese la contraseña para el usuario {usr} (aviso, no se oculta el input): ')
    return host, db, usr, passw, logmech


host, db, usr, passw, logmech = ask_user_pwd()
logger.info('Generando dataframe de prueba')
test_df: DataFrame = generate_dataframe(num_rows=500000)
logger.info('Realizando conexiones a la base de datos')
t_start = time()
teradata = TeradataDB(host=host, user=usr, passw=passw, db=db, logmech=logmech)
t_conn = time() - t_start
logger.info(f'Tiempo de conexión: {round(t_conn, 2)} s')
logger.info('Conexión exitosa')


class TeradataTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.td = teradata

    def test_date(self):
        logger.info('Obteniendo fecha del servidor de la base de datos')
        t_start = time()
        logger.info(f'La fecha del servidor de la base de datos es: {self.td.current_date().strftime("%Y-%m-%d")}')
        t_qry = time() - t_start
        logger.info(f'Tiempo de lectura: {round(t_qry, 2)} s')

    def test_fastload(self):
        schema = 'p_staging'
        table = 'STG_TERADATAML_FASTLOAD_TEST'
        try:
            self.td.drop_table(schema, table)
        except pyodbc.ProgrammingError:
            pass
        logger.info(f'Escribiendo tabla con {len(test_df)} filas vía Fastload')
        t_start = time()
        self.td.retry_fastload(test_df, schema, table, pk='ID')
        t_qry = time() - t_start
        logger.info(f'La carga tardó {round(t_qry, 2)} s')
        self.odbc(table)

    def odbc(self, flname):
        schema = 'p_staging'
        table = 'STG_TERADATAML_ODBC_TEST'
        try:
            self.td.drop_table(schema, table)
        except pyodbc.ProgrammingError:
            pass
        logger.info(f'Realizando copia de la DDL de la tabla creada con Fastload')
        self.td.create_table_like(schema, table, schema, flname)
        logger.info(f'Escribiendo tabla con {len(test_df)} filas vía ODBC')
        t_start = time()
        test_df.to_sql(name=table, con=self.td.engine, schema=schema, if_exists='append', index=False)
        t_qry = time() - t_start
        logger.info(f'La carga tardó {round(t_qry, 2)} s')


if __name__ == '__main__':
    unittest.main()
