import unittest
from libgal.modules.Logger import Logger
from libgal import teradata, TeradataError
from libgal.modules.Utils import ask_user_pwd

logger = Logger().get_logger()


host, usr, passw, logmech = ask_user_pwd()
conexion = teradata(host=host, username=usr, password=passw, logmech=logmech)


class TeradataBasicTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.td = conexion

    def test_exception(self):
        logger.info('Iniciando test de excepción TeradataError')
        try:
            data = ('1', 'Descripción 1')
            query = "INSERT INTO esquema.tabla(codigo, descripcion) VALUES (?,?)"

            with self.td.cursor() as cursor:
                cursor.execute(query, data)
                self.td.commit()

            logger.error("Los datos fueron almacenados correctmente.")
            assert False, "No se generó la excepción esperada."

        except TeradataError as e:
            logger.info("Ocurrió un error (esperado) al intentar almacenar los datos.")
            logger.info(e)

    def test_no_exception(self):
        logger.info('Iniciando test sin excepción TeradataError')
        try:
            queries = ["CREATE TABLE p_staging.libgal_tabla (codigo INTEGER, descripcion VARCHAR(100))",
                       "INSERT INTO p_staging.libgal_tabla(codigo, descripcion) VALUES (?,?)",
                       "DROP TABLE p_staging.libgal_tabla"]

            data = ('1', 'Descripción 1')
            with self.td.cursor() as cursor:
                for query in queries:
                    logger.info(f'Ejecutando query: {query}')
                    if 'INSERT' in query:
                        cursor.execute(query, data)
                    else:
                        cursor.execute(query)
                    self.td.commit()

            logger.info("Los datos fueron almacenados correctmente.")

        except TeradataError as e:
            logger.info("Ocurrió un error al intentar almacenar los datos.")
            logger.error(e)
            assert False


if __name__ == '__main__':
    unittest.main()
