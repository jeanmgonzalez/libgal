from libgal.modules.SqliteDB import SqliteDB
from libgal.modules.Logger import Logger

logger = Logger().get_logger()


class SQLMemoryDB(SqliteDB):

    def __init__(self, dbfile):
        self.dbfile = dbfile
        super().__init__(dbfile=':memory:')

    def create_tables(self):
        pass

    def vacuum(self):
        logger.info(f'Volcando memoria a {self.dbfile}')
        self.do(f"vacuum main into '{self.dbfile}'")
