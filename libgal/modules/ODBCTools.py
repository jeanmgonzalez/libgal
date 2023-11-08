import re
import pandas as pd
import datetime
import math

def select_records_by_range(conn, table, index_columns, range_column=None, range_values=None, schema=None):

    if isinstance(index_columns, list):
        index_columns = [f'"{col}"' for col in index_columns]
        selected_columns = ", ".join(index_columns)
    else:
        selected_columns = index_columns

    if schema is not None:
        table_name = f'{schema}.{table}'
    else:
        table_name = table

    if range_values is not None:
        assert range_column is not None, "range_column is expected to have name if range_values are specified"
        assert isinstance(range_values, list), "range_values should be list"
        range_start = min(range_values)
        range_end = max(range_values)
        query = f'SELECT {selected_columns} FROM {table_name} WHERE {range_column} >= {range_start} AND {range_column} <= {range_end};'
    else:
        query = f'SELECT {selected_columns} FROM {table_name};'

    return pd.read_sql(query, conn)


def insert_dataframe(conn, table, dataframe, primary_key='id', optional_schema=None):
    if not dataframe.empty:
        pk_range = [min(dataframe[primary_key]), max(dataframe[primary_key])]
        range_column = primary_key
        index_columns = dataframe.columns.tolist()
        existing_records = select_records_by_range(conn, table, index_columns, range_column, pk_range, optional_schema)

        if existing_records.empty:
            dataframe.to_sql(table, conn, if_exists='append', index=False, schema=optional_schema)
            result_df = dataframe
        else:
            for column in index_columns:
                if pd.api.types.is_numeric_dtype(dataframe[column]):
                    existing_records[column] = pd.to_numeric(existing_records[column])
            result_df = dataframe.merge(existing_records, on=index_columns, how='left')
            if not result_df.empty:
                result_df.to_sql(table, conn, if_exists='append', index=False, schema=optional_schema)

        return result_df, existing_records
    else:
        return dataframe, dataframe


def insert_by_primary_key(conn, table, dataframe, primary_key='id', schema=None):
    if not dataframe.empty:
        existing_records = select_records_by_range(conn, table, primary_key, schema=schema)
        if existing_records.empty:
            dataframe.to_sql(table, conn, if_exists='append', index=False, schema=schema)
            result_df = dataframe
        else:
            result_df = dataframe[~dataframe[primary_key].isin(existing_records[primary_key])]
            result_df.to_sql(table, conn, if_exists='append', index=False, schema=schema)

        return result_df, existing_records
    else:
        return dataframe, dataframe


def execute(query, conn):
    c = conn.cursor()
    c.execute(query)
    c.close()
    conn.commit()


def upsert_by_primary_key(conn, table, dataframe, primary_key='id', schema=None):
    if not dataframe.empty:
        if pd.api.types.is_numeric_dtype(dataframe[primary_key]):
            pk_in_list = ','.join(dataframe[primary_key].unique().astype(dtype=str).tolist())
        else:
            pk_in_list = "'" + "','".join(dataframe[primary_key].unique().astype(dtype=str).tolist()) + "'"

        if schema is not None:
            escaped_table_name = f'{schema}.{table}'
        else:
            escaped_table_name = f'{table}' if '.' not in table else f'"{table}"'

        query = f"DELETE FROM {escaped_table_name} WHERE {primary_key} IN ({pk_in_list});"
        execute(query, conn.raw_connection())
        return insert_by_primary_key(conn, table, dataframe, primary_key, schema)


def load_table(conn, table, use_quotes=True):
    if use_quotes:
        return pd.read_sql(sql=f'SELECT * FROM "{table}"', con=conn, index_col=None, coerce_float=True, parse_dates=None, columns=None, chunksize=None)
    else:
        return pd.read_sql(sql=f'SELECT * FROM {table}', con=conn, index_col=None, coerce_float=True, parse_dates=None, columns=None, chunksize=None)


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

