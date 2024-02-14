# SQLALchemy
from typing import Optional
from pandas import DataFrame
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError as SQLAlchemyError
from libgal.modules.Teradata import TeradataML
from libgal.modules.DatabaseAPI import DatabaseAPI, FunctionNotImplementedException


class SQLAlchemy(DatabaseAPI):
    """
    Descripción: Permite la conexión hacia la Base de Datos
    Parámetros:
    - driver (String): Tipo de conexión o base de datos a utilizar
    - host (String): uri del servidor de base de datos
    - username (String): Usuario que autentica la conexión a la base de datos
    - password (String): Contraseña para la autenticación de la connexión de la base de datos
    - logmech (String): Parámetro Opcional que indica el método de autenticación del usuario. LDAP por defecto
    """

    def __init__(self, driver, host, username, password, logmech="LDAP", timeout_seconds=None, pool_recycle=1800,
                 pool_size=20):

        from libgal.modules.Logger import Logger
        self.logger = Logger().get_logger()
        self.driver = driver
        self.host = host
        self.username = username
        self.password = password
        self.logmech = logmech
        self.pool_size = pool_size
        self.pool_recycle = pool_recycle
        self.timeout_seconds = timeout_seconds
        self._engine = None
        self.connect()
        self._session = None

    def connect(self):
        if self.driver.lower() == "teradata":
            self._engine = TeradataML(host=self.host, user=self.username, passw=self.password, logmech=self.logmech).engine
        elif self.driver.lower() == "mysql":
            self._engine = create_engine(f"mysql+mysqlconnector://{self.username}:{self.password}@{self.host}/",
                                        pool_recycle=self.pool_recycle, pool_size=self.pool_size)
        else:
            raise ValueError(f"El driver {self.driver} de base de datos no está soportado")

    @property
    def engine(self):
        return self._engine

    @property
    def Session(self):
        self._session = sessionmaker(bind=self.engine)
        return self._session

    @property
    def Base(self):
        return declarative_base()

    def query(self, query):
        """
        Descripción: Permite ejecutar una instrucción SQL según el motor de Base de Datos.
        Parámetro:
        - query (String): Instrucción SQL a ejecutar
        """
        with self.engine.connect() as conn:
            return conn.execute(text(query))

    def insert(self, pandas_dataframe, database, table, pk=None, parser_limit=10000):
        """
        Descripción: Permite ejecutar una instrucción SQL según el motor de Base de Datos.
        Parámetro:
        - pandas_dataframe: Dataframe de Pandas que contiene la info a insertar
        - database (String): Base de datos que contiene la tabla a poblar.
        - table (String): Tabla donde se insertaran los datos del Dataframe
        """
        with self.engine.connect() as conn:
            pandas_dataframe = pandas_dataframe.astype(str)

            try:
                pandas_dataframe.to_sql(table, schema=database, con=conn, if_exists='append', index=False)
            except SQLAlchemyError as e:
                print(e)

    def InsertDataframe(self, pandas_dataframe, database, table, pk=None, parser_limit=10000):
        """
        Descripción: Permite ejecutar una instrucción SQL según el motor de Base de Datos.
        Parámetro:
        - pandas_dataframe: Dataframe de Pandas que contiene la info a insertar
        - database (String): Base de datos que contiene la tabla a poblar.
        - table (String): Tabla donde se insertaran los datos del Dataframe
        """
        self.logger.warning("FutureWarning: El método InsertDataframe será eliminado en futuras versiones. Utilice el método insert")
        self.insert(pandas_dataframe, database, table, pk, parser_limit)

    def do(self, query):
        raise FunctionNotImplementedException("do")

    def drop_table(self, schema, table):
        raise FunctionNotImplementedException("drop_table")

    def truncate_table(self, schema, table):
        raise FunctionNotImplementedException("truncate_table")

    def table_columns(self, schema, table):
        raise FunctionNotImplementedException("table_columns")

    def upsert(self, df: DataFrame, schema: Optional[str], table: str, pk: str):
        raise FunctionNotImplementedException("upsert")

    def diff(self, schema_src: Optional[str], table_src: str,
             schema_dst: Optional[str], table_dst: str) -> DataFrame:
        raise FunctionNotImplementedException("diff")

    def staging_insert(self, df: DataFrame, schema_src: Optional[str], table_src: str,
                       schema_dst: Optional[str], table_dst: str, pk: str):
        raise FunctionNotImplementedException("staging_insert")

    def staging_upsert(self, df: DataFrame, schema_src: Optional[str], table_src: str,
                        schema_dst: Optional[str], table_dst: str, pk: str):
          raise FunctionNotImplementedException("staging_upsert")
