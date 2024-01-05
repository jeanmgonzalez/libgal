# SQLALchemy
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError as SQLAlchemyError
from libgal.modules.Teradata import Teradata


class SQLAlchemy:
    """
    Descripción: Permite la conexion hacia la Base de Datos
    Parámetros:
    - driver (String): Tipo de conexión o base de datos a utilizar
    - host (String): uri del servidor de base de datos
    - username (String): Usuario que autentica la conexión a la base de datos
    - password (String): Contraseña para la autenticación de la connexión de la base de datos
    - logmech (String): Parámetro Opcional que indica el método de autenticación del usuario. LDAP por defecto
    """

    def __init__(self, driver, host, username, password, logmech="LDAP", timeout_seconds=None, pool_recycle=1800,
                 pool_size=20):

        if driver.lower() == "teradata":
            self._engine = Teradata(host=host, user=username, passw=password, logmech=logmech).engine
        elif driver.lower() == "mysql":
            self._engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}/",
                                        pool_recycle=pool_recycle, pool_size=pool_size)

        self._session = None

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

    def InsertDataframe(self, pandas_dataframe, database, table):
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

