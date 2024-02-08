import hashlib
import string
from datetime import datetime
from typing import Optional, List
from pandas import DataFrame
import numpy as np


def drop_lists(df: DataFrame) -> DataFrame:
    """
        Esta función elimina las celdas con listas del dataframe.
        :param df: el dataframe
        :return: el dataframe sin las celdas con listas
    """
    to_drop = list()
    for attribute_name, order_data in df.items():
        for element in df[attribute_name]:
            if isinstance(element, list):
                to_drop.append(attribute_name)
                break

    return df.drop(
        to_drop,
        axis=1, errors='ignore'
    )


def chunks(lst: list, n: int) -> list:
    """
        Esta función divide una lista en porciones de tamaño n.
        :param lst: la lista
        :param n: el tamaño de las porciones
        :return: la lista dividida en porciones de tamaño n
    """

    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def chunks_df(df: DataFrame, n: int) -> List[DataFrame]:
    """
        Esta función divide un dataframe en porciones de tamaño n.
        :param df: el dataframe
        :param n: el tamaño de las porciones
        :return: el dataframe dividido en porciones de tamaño n
    """
    chunk_size = max(int(len(df) / n), 1)
    return np.array_split(df, chunk_size)


def remove_non_latin1(a_str: Optional[str]) -> Optional[str]:
    """
        Esta función elimina los caracteres no latin1 del string.
        :param a_str: el string
        :return: el string sin los caracteres no latinos
    """
    if a_str is None:
        return a_str
    latin1_extensions = ''.join([chr(x) for x in range(161, 255)])
    latin1_chars = set(string.printable + latin1_extensions)
    replace_chars, replacement_chars = ['´', '`'], ["'", "'"]
    for i, char in enumerate(replace_chars):
        a_str = a_str.replace(replace_chars[i], replacement_chars[i])
    return ''.join(
        filter(lambda x: x in latin1_chars, a_str)
    )


def powercenter_compat_df(message: DataFrame) -> DataFrame:
    """
        Esta función devuelve un dataframe compatible con FlatFile de PWC.
        :param message: el dataframe a transformar
        :return: el dataframe compatible con FlatFile de PWC
    """
    return message.replace(
        to_replace=[r"\\t|\\n|\\r|\|", "\t|\n|\r"],
        value=[' ', ' '],
        regex=True,
    )


def powercenter_compat_str(message: str) -> str:
    """
        Esta función devuelve un string compatible con FlatFile de PWC.
        Elimina todos los caracteres de control como retorno de carro, tabs, pipes, etc.
        :param message: el mensaje a transformar
        :return: el string compatible con FlatFile de PWC
    """
    replace_chars, replacement_chars = ['\\t', '\\n', '\\r', '|', '\t', '\n', '\r'], [' ', ' ', ' ', ' ', ' ', ' ', ' ']
    for i, char in enumerate(replace_chars):
        message = message.replace(replace_chars[i], replacement_chars[i])

    return message


def hash_primary_key(
        row: dict,
        fields: list,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = '%Y-%m-%d',
        trim: Optional[int] = None
) -> str:
    """
        Esta función es para generar una clave única para una fila de una tabla utilizando un hash SHA-256.
            :param row: el registro/fila de la tabla
            :param fields: los campos/columnas que se usarán para generar la clave
            :param timestamp_field: el campo que contiene la fecha y hora del registro (opcional)
            :param timestamp_format: el formato de fecha y hora del campo (opcional)
                si timestamp_format es 'iso' se asume que el campo es una cadena con formato ISO 8601
            :param trim: la cantidad de caracteres que se tomarán del hash generado
            :return: la clave única
    """
    string = ''.join([row[field] for field in fields])
    sha256_string_hash_hex = hashlib.sha256(string.encode()).hexdigest()
    if timestamp_field is None:
        if trim is None:
            return sha256_string_hash_hex
        else:
            return sha256_string_hash_hex[0:trim]
    else:
        # Si se usa un campo de fecha y hora, se agrega el timestamp al hash
        # y se toman los primeros 12 caracteres del hash (excepto que se especifique otro valor)
        if trim is None:
            trim = 12
        if timestamp_format.lower() == 'iso' or timestamp_format.lower() == 'iso8601':
            unix_epoch = datetime.fromisoformat(row[timestamp_field]).timestamp()
        else:
            unix_epoch = datetime.strptime(row[timestamp_field], timestamp_format).timestamp()
        return f"{int(unix_epoch)}_{sha256_string_hash_hex[0:trim]}"

