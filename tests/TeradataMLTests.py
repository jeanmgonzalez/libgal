import unittest

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


class TeradataTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        host, db, usr, passw, logmech = ask_user_pwd()
        logger.info('Realizando conexiones a la base de datos')
        self.td = TeradataDB(host=host, user=usr, passw=passw, db=db, logmech=logmech)
        logger.info('Conexión exitosa')

    def test_date(self):
        logger.info('Obteniendo fecha del servidor de la base de datos')
        logger.info(f'La fecha del servidor la base de datos es: {self.td.current_date().strftime("%Y-%m-%d")}')


if __name__ == '__main__':
    unittest.main()
