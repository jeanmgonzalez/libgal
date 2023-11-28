import unittest
from time import time
from pandas import DataFrame
from libgal.modules.Logger import Logger
from test_dataframe import generate_dataframe
from libgal.modules.SQLMemory import SQLMemory
import os

logger = Logger().get_logger()

DB_FILENAME = 'sqlite_test_staging.db'
logger.info('Generando dataframe de prueba')
test_df: DataFrame = generate_dataframe(num_rows=500000)
logger.info('Realizando conexiones a la base de datos')
t_start = time()
sql = SQLMemory(dbfile=DB_FILENAME)
t_conn = time() - t_start
logger.info(f'Tiempo de conexión: {round(t_conn, 2)} s')
logger.info('Conexión exitosa')


class SQLiteTests(unittest.TestCase):

    def test_sqlite(self):
        tabla = 'test_table'
        tabla_copy = 'copy_table'
        schema = None
        pk = 'Log_Id'
        self.insert(schema, tabla, pk)
        self.copy(tabla, tabla_copy, pk)
        assert self.verify_tables(schema, tabla, schema, tabla_copy), "Las tablas difieren"
        self.delete(tabla, pk)
        assert not self.verify_tables(schema, tabla_copy, schema, tabla), "diff funciona mal"
        self.upsert(schema, tabla, pk)
        assert self.verify_tables(schema, tabla, schema, tabla_copy), "Falló el upsert"
        self.dump()

    def copy(self, tabla, tabla_copy, pk):
        sql.create_table_like(None, tabla_copy, None, tabla)
        sql.insert(test_df, None, tabla_copy, pk)

    def dump(self):
        logger.info('Volcando archivo')
        t_start = time()
        sql.vacuum()
        t_tx = time() - t_start
        logger.info(f'Tiempo de escritura {round(t_tx,2)} s')
        os.unlink(DB_FILENAME)

    def delete(self, table, pk):
        query = f'DELETE FROM {table} WHERE {pk} < 100;'
        sql.do(query)

    def insert(self, schema, tabla, pk):
        logger.info('Realizando carga (insert)')
        t_start = time()
        sql.insert(test_df, schema, tabla, pk)
        t_tx = time() - t_start
        logger.info(f'Tiempo de carga {round(t_tx,2)} s')

    def upsert(self, schema, tabla, pk):
        logger.info('Realizando carga (upsert)')
        t_start = time()
        sql.insert(test_df, schema, tabla, pk)
        t_tx = time() - t_start
        logger.info(f'Tiempo de carga {round(t_tx,2)} s')

    def verify_tables(self, schema_a, a, schema_b, b):
        difference = sql.diff(schema_a, a, schema_b, b)
        if len(difference) > 0:
            logger.error("Las tablas difieren en calidad o cantidad de registros")
            logger.debug(difference.to_dict())
            return False
        return True


if __name__ == '__main__':
    unittest.main()
