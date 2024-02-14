import unittest
from time import time
from sqlalchemy import Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
import libgal
from pandas import DataFrame
from libgal.modules.Logger import Logger
from libgal.modules.Utils import generate_dataframe, ask_user_pwd


logger = Logger().get_logger()


# Defino clase de la tabla
class ClaseTabla(declarative_base(metadata=MetaData())):
    __tablename__ = 'libgal_sqlalchemy_test'
    __table_args__ = {'schema': 'p_staging'}
    campo_clave = libgal.Column(libgal.Integer, primary_key=True)
    descripcion = libgal.Column(libgal.String)

    def __repr__(self):
        return f"<dato(campo_clave='{self.campo_clave}', descripcion={self.descripcion})>"


host, usr, passw, logmech = ask_user_pwd()
logger.info('Generando dataframe de prueba')
test_df: DataFrame = generate_dataframe(num_rows=500000)
logger.info('Realizando conexiones a la base de datos')
con = libgal.sqlalchemy(host=host, username=usr, password=passw, driver='teradata', logmech=logmech)
logger.info('Conexión exitosa')


class SQLAlchemyTests(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.con = con

    def create_table_ddl(self):
        metadata = MetaData()
        clase_tabla = Table(
            ClaseTabla.__tablename__,
            metadata,
            libgal.Column('campo_clave', libgal.Integer, primary_key=True),
            libgal.Column('descripcion', libgal.String),
            schema='p_staging'
        )
        return clase_tabla, metadata

    def test_create_query_drop(self):
        logger.info('Iniciando Test de inserción y query por SQLAlchemy')
        table, metadata = self.create_table_ddl()

        # Crea la tabla
        logger.info(f'Creando tabla {ClaseTabla.__tablename__}')
        metadata.create_all(self.con.engine)

        session = self.con.Session()

        # Agrega un campo
        logger.info(f'Insertando datos en tabla {ClaseTabla.__tablename__}')
        nuevo_dato = ClaseTabla(campo_clave=1, descripcion='Descripción del registro')
        session.add(nuevo_dato)
        session.commit()

        # Verifica el campo insertado
        logger.info(f'Validando inserción en tabla {ClaseTabla.__tablename__}')
        query_tabla = session.query(ClaseTabla)
        datos = query_tabla.all()
        assert len(datos) > 0, 'No se pudieron leer los datos insertados'

        # borra la tabla
        logger.info(f'Borrando tabla {ClaseTabla.__tablename__}')
        table.drop(self.con.engine)

        session.close()

    def test_insert_dataframe(self):
        logger.info('Iniciando Test de inserción de dataframe por SQLAlchemy')
        table_schema = 'p_staging'
        table_name = 'libgal_sqlalchemy_dataframe_test'

        # Crea la tabla desde el dataframe
        df_structure_only = test_df.drop(test_df.index)
        df_structure_only.to_sql(table_name, schema=table_schema, con=self.con.engine, if_exists='replace', index=False)

        # Inserta el dataframe
        t_start = time()
        logger.info(f'Insertando datos en tabla {table_schema}.{table_name}')
        self.con.insert(test_df, table_schema, table_name)
        elapsed = time() - t_start
        logger.info(f'Tiempo de inserción: {elapsed: .2f} segundos')

        # Borra la tabla
        logger.info(f'Borrando tabla {table_schema}.{table_name}')
        self.con.query(f'DROP TABLE {table_schema}.{table_name}')

    def test_sqalchemyerror(self):
        session = self.con.Session()
        try:
            nuevo_dato = ClaseTabla(campo_clave=1, descripcion='Descripción del registro')
            session.add(nuevo_dato)
            session.commit()
            logger.error("Datos almacenados correctamente")
            assert False, "No se generó el error esperado"

        except libgal.SQLAlchemyError as e:
            session.rollback()

        session.close()


if __name__ == '__main__':
    unittest.main()
