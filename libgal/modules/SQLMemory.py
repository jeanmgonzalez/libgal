from libgal.modules.Sqlite import Sqlite
from libgal.modules.Logger import Logger

logger = Logger().get_logger()


class SQLMemory(Sqlite):

    def __init__(self, dbfile):
        """
        Crea una base de datos en memoria
            :param dbfile: Nombre del archivo a volcar en vacuum()
        """
        self.dbfile = dbfile
        super().__init__(dbfile=':memory:')

    def vacuum(self):
        """
        Vuelca la memoria a un archivo
        """
        logger.info(f'Volcando memoria a {self.dbfile}')
        self.do(f"vacuum main into '{self.dbfile}'")
