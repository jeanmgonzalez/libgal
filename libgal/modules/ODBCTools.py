import re
import pandas as pd
import datetime
import math


def load_table(conn, table, use_quotes=True):
    if use_quotes:
        return pd.read_sql(sql=f'SELECT * FROM "{table}"', con=conn, index_col=None, coerce_float=True,
                           parse_dates=None, columns=None, chunksize=None)
    else:
        return pd.read_sql(sql=f'SELECT * FROM {table}', con=conn, index_col=None, coerce_float=True,
                           parse_dates=None, columns=None, chunksize=None)


def load_sql(path):
    with open(path, mode='r', encoding='utf-8') as file:
        data = file.read().replace('\n', ' ')
    data = re.sub(r'^--.*$', '', data)
    data_split = data.split(';')
    return_queries = []
    for query in data_split:
        if len(query.strip()) > 0:
            return_queries.append(query)
    return return_queries


def inserts_from_dataframe(source, target):
    sql_texts = []
    for index, row in source.iterrows():
        try:
            for name, value in row.items():
                if isinstance(value, datetime.date):
                    row[name] = value.strftime('%Y-%m-%d')
                elif isinstance(value, datetime.time):
                    row[name] = value.strftime('%H:%M:%S')
                elif value is None or (isinstance(value, float) and math.isnan(value)):
                    row[name] = 'NULL_NONE'
                elif isinstance(value, str):
                    row[name] = value.replace("'", '').strip()
                elif ('Id' in name or 'Num' in name) and value == int(value):
                    row[name] = int(value)
        except ValueError as e:
            print(e)
            print(row)
            exit(1)

        insert = 'INSERT INTO ' + target + ' (' + str(', '.join(source.columns)) + ') VALUES ' + str(tuple(row.load_values)) + ';'
        sql_texts.append(insert.replace("'NULL_NONE'", 'NULL'))
    return sql_texts

