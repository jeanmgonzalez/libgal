import unittest
from time import time
from pandas import DataFrame
from libgal.modules.Logger import Logger
from test_dataframe import generate_dataframe
from libgal.modules.SQLMemoryDB import SQLMemoryDB
import os

logger = Logger().get_logger()

DB_FILENAME = 'sqlite_test_staging.db'
logger.info('Generando dataframe de prueba')
test_df: DataFrame = generate_dataframe(num_rows=500000)
logger.info('Realizando conexiones a la base de datos')
t_start = time()
sql = SQLMemoryDB(dbfile=DB_FILENAME)
t_conn = time() - t_start
logger.info(f'Tiempo de conexión: {round(t_conn, 2)} s')
logger.info('Conexión exitosa')


class SQLiteTests(unittest.TestCase):

    def test_sqlite(self):
        logger.info('Realizando carga')
        t_start = time()
        test_df.to_sql('test_table', sql.engine, schema=None, if_exists='replace', index=False)
        t_tx = time() - t_start
        logger.info(f'Tiempo de carga {round(t_tx,2)} s')
        logger.info('Volcando archivo')
        t_start = time()
        sql.vacuum()
        t_tx = time() - t_start
        logger.info(f'Tiempo de escritura {round(t_tx,2)} s')
        os.unlink(DB_FILENAME)


if __name__ == '__main__':
    unittest.main()
