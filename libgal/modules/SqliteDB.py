from abc import abstractmethod
import pandas as pd
import sqlalchemy
from libgal.modules.Logger import Logger
import re
from libgal.modules.ODBCTools import upsert_by_primary_key, load_table, load_sql

logger = Logger.__call__().get_logger()


class SqliteDB:

    def __init__(self, dbfile, drop_tables=False):
        self.eng = sqlalchemy.create_engine(f'sqlite:///{dbfile}')
        self.conn = self.eng.raw_connection()
        self.should_drop_tables = drop_tables
        self.create_tables()

    def do(self, query):
        c = self.conn.cursor()
        logger.info(f'Ejecutando query: {query}')
        c.execute(query)
        c.close()
        self.conn.commit()

    def load_table(self, table):
        return load_table(self.engine, table)

    def upsert_dataframe(self, table, df, column='id'):
        return upsert_by_primary_key(self.eng, table, df, column)

    def upsert_by_primary_key(self, table, df, column='id'):
        return upsert_by_primary_key(self.eng, table, df, column)

    def load_sql(self, path):
        return load_sql(path)

    def query_df(self, query):
        return pd.read_sql(sql=query, con=self.engine, index_col=None, coerce_float=True, parse_dates=None, columns=None, chunksize=None)

    def query_table_cols(self, query):
        return self.query_df(query).columns

    @staticmethod
    def cursor_execute(c, query_split):
        for query in query_split:
            if len(query) > 0:
                c.execute(query + ';')

    def drop_tables(self, tables):
        c = self.conn.cursor()
        for table in tables:
            query = f'DROP TABLE IF EXISTS "{table}";'
            c.execute(query)
        self.conn.commit()

    def drop_views(self, views):
        c = self.conn.cursor()
        for view in views:
            query = f'DROP VIEW IF EXISTS "{view}";'
            c.execute(query)
        self.conn.commit()

    @abstractmethod
    def create_tables(self):
        return

    def strip_names(self, df, name):
        col_names = df.columns
        new_names = [re.sub(name, '', col) for col in col_names]
        dict_names = dict(zip(col_names, new_names))
        renamed = df.rename(columns=dict_names)
        return renamed, col_names

    def select_path(self, df, pattern):
        to_strip = df.iloc[:, df.columns.str.contains(f'^{pattern}\..*')]
        return self.strip_names(to_strip, f'^{pattern}\.')

    def select_cols(self, df, pattern):
        to_strip = df.iloc[:, df.columns.str.contains(f'^{pattern}.*')]
        return self.strip_names(to_strip, f'^{pattern}')

    def key_exists(self, table, field_id, key):
        query = f'SELECT * FROM {table} WHERE {field_id}="{key}";'
        result_df = pd.read_sql_query(query, self.conn)
        return not result_df.empty

    # TODO: extraer los list en otro df
    @staticmethod
    def drop_lists(df):
        to_drop = list()
        for attribute_name, order_data in df.items():
            for element in df[attribute_name]:
                if isinstance(element, list):
                    to_drop.append(attribute_name)
                    break

        return df.drop(
            to_drop,
            axis=1, errors='ignore'
        ), None

    @property
    def connection(self):
        return self.conn

    @property
    def engine(self):
        return self.eng

