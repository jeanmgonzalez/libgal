import unittest
from time import time
from pandas import DataFrame
from test_dataframe import generate_dataframe
from libgal.modules.Teradata import Teradata
from libgal.modules.Logger import Logger
from TestsUtils import ask_user_pwd

logger = Logger().get_logger()

host, usr, passw, logmech = ask_user_pwd()
logger.info('Generando dataframe de prueba')
test_df: DataFrame = generate_dataframe(num_rows=500000)
logger.info('Realizando conexiones a la base de datos')
t_st = time()
teradata = Teradata(host=host, user=usr, passw=passw, logmech=logmech)
t_conn = time() - t_st
logger.info('Conexión exitosa')
logger.info(f'Tiempo de conexión: {round(t_conn, 2)} s')


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

    def test(self):
        schema = 'p_staging'
        dl_schema = 'p_dw_tables'
        flname = 'STG_TERADATAML_FASTLOAD_TEST_V2'
        dbcname = 'STG_TERADATAML_ODBC_TEST_V2'
        dw_table = 'STG_TERADATAML_STAGINGLOAD_TEST_V2'
        self.staging_load(schema, dbcname, dl_schema, dw_table, 'Log_Id')
        self.fastload(schema, flname)
        self.verify_tables(schema, flname, dl_schema, dw_table)
        self.odbc(schema, dbcname, flname)
        self.verify_tables(schema, dbcname, dl_schema, dw_table)
        self.post_checks(schema, flname, dbcname)
        yn = input('Desea borrar las tablas de prueba generadas (s/n)? ')
        if yn.lower() == 's':
            self.cleanup(schema, [dbcname, flname])
            self.cleanup(dl_schema, [dw_table])

    def staging_load(self, schema, dbcname, dl_schema, dw_table, pk):
        t_start = time()
        self.td.staging_insert(test_df, schema, dbcname, dl_schema, dw_table, pk)
        t_qry = time() - t_start
        logger.info(f'La carga (insert) tardó {round(t_qry, 2)} s')

        t_start = time()
        self.td.staging_upsert(test_df, schema, dbcname, dl_schema, dw_table, pk)
        t_qry = time() - t_start
        logger.info(f'La carga (upsert) tardó {round(t_qry, 2)} s')

    def fastload(self, schema, table):
        self.td.drop_table_if_exists(schema, table)
        logger.info(f'Escribiendo tabla con {len(test_df)} filas vía Fastload')

        t_start = time()
        self.td.retry_fastload(df=test_df, schema=schema, table=table, pk='Log_Id')
        t_qry = time() - t_start
        logger.info(f'La carga tardó {round(t_qry, 2)} s')

    def odbc(self, schema, table, flname):
        self.td.drop_table_if_exists(schema, table)
        logger.info(f'Realizando copia de la DDL de la tabla creada con Fastload')
        self.td.create_table_like(schema, table, schema, flname)
        logger.info(f'Escribiendo tabla con {len(test_df)} filas vía insert_dataframe')
        t_start = time()
        self.td.insert(test_df, schema, table, 'Log_Id')
        t_qry = time() - t_start
        logger.info(f'La carga tardó {round(t_qry, 2)} s')

    def cleanup(self, schema, table_list):
        for table in table_list:
            self.td.drop_table(schema, table)

    def verify_tables(self, schema, fltable, schema_b, dbctable):
        difference = self.td.diff(schema, fltable, schema_b, dbctable)
        if len(difference) > 0:
            logger.error("Las tablas difieren en calidad o cantidad de registros")
            logger.debug(difference.to_dict())
            return False
        return True

    def upsert_stage(self, schema, fltable, dbctable):
        logger.info(f'Verificando UPSERT')
        t_start = time()
        self.td.upsert(df=test_df, schema=schema, table=dbctable, pk='Log_Id', odbc_limit=len(test_df))
        t_qry = time() - t_start
        logger.info(f'La carga tardó {round(t_qry, 2)} s')
        assert self.verify_tables(schema, fltable, schema, dbctable)

    def delete(self, schema, table, pk):
        self.td.do(f'DELETE FROM {schema}.{table} WHERE {pk} BETWEEN 1 AND 100;')

    def post_checks(self, schema, fltable, dbctable):
        logger.info('Verificando si hay diferencias de registros entre tablas')
        assert self.verify_tables(schema, fltable, schema, dbctable)
        self.delete(schema, dbctable, 'Log_Id')
        assert not self.verify_tables(schema, fltable, schema, dbctable), \
            "La función de comparación de tablas no funciona correctamente"
        self.upsert_stage(schema, fltable, dbctable)


if __name__ == '__main__':
    unittest.main()
