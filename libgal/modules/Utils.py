import hashlib
import string
from typing import Optional, List
from pandas import DataFrame
import numpy as np
import pandas as pd
import random
from datetime import datetime, timedelta
from getpass import getpass


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
    string = ''.join([str(row[field]) for field in fields])
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
            unix_epoch = datetime.fromisoformat(str(row[timestamp_field])).timestamp()
        else:
            unix_epoch = datetime.strptime(str(row[timestamp_field]), timestamp_format).timestamp()
        return f"{int(unix_epoch)}_{sha256_string_hash_hex[0:trim]}"


def generate_dataframe(num_rows=1000000):
    """
        Esta función genera un dataframe de prueba con datos aleatorios.
        :param num_rows: la cantidad de filas del dataframe
        :return: el dataframe de prueba
    """
    nombres_animales = ['áspid', 'colibrí', 'tejón', 'mújol', 'tálamo', 'coendú', 'vicuña', 'ñandú', 'alacrán',
                        'armiño',
                        'pingüino', 'delfín', 'galápago', 'tiburón', 'murciélago', 'águila', 'ácana', 'tábano',
                        'caimán',
                        'tórtola', 'zángano', 'búfalo', 'dóberman', 'aúreo', 'cóndor', 'camaleón', 'nandú', 'órix',
                        'tucán', 'búho', 'pájaro']

    nombres_personas = ['Valentina', 'Mateo', 'Laura', 'Camila', 'Renata', 'Sara', 'Amelia', 'Nicolás', 'Valery',
                        'Julio',
                        'Fernanda', 'Santiago', 'Isabella', 'Isabel', 'Joaquín', 'Emmanuel', 'Luciana', 'Ariana',
                        'Valeria', 'Dylan', 'Daniel', 'Juan', 'Lucas', 'Mariana', 'Sofía', 'Alejandro', 'Emma',
                        'Carlos', 'Ángel', 'Ana', 'Benjamín', 'Fabiola', 'Sebastián', 'Antonella', 'Gabriela', 'Diego',
                        'Esteban', 'Olivia', 'Emily', 'Adrián', 'Matías', 'Mía', 'Samuel', 'Leonardo', 'Emiliano',
                        'Gabriel', 'Victoria', 'Juliana']

    apellidos_personas = ['Cruz', 'González', 'López', 'Herrera', 'Rivera', 'Molina', 'Rojas', 'Delgado', 'Ruiz',
                          'Sánchez', 'Castillo', 'Peralta', 'Guzmán', 'Pérez', 'Vargas', 'Vásquez', 'Castro',
                          'Silva', 'Romero', 'Gutiérrez', 'Ortiz', 'Mendoza', 'Álvarez', 'Ramírez', 'Ortega',
                          'Aguilar', 'Chávez', 'Núñez', 'Rodríguez', 'Padilla', 'Díaz', 'Gómez', 'Guerrero',
                          'Torres', 'García', 'Hernández', 'Morales', 'Reyes', 'Flores', 'Martínez', 'Campos',
                          'Jiménez', 'Estrada', 'Ramos']

    # Columna 1: Fecha
    start_date = datetime(2024, 1, 1)
    date_column = [start_date + timedelta(seconds=i*300) for i in range(num_rows)]

    # Columna 2: Identificador único incremental
    id_column = list(range(1, num_rows + 1))

    # Columna 3: Nombre
    name_column = [random.choice(nombres_personas) for _ in range(num_rows)]

    # Columna 4: Apellido
    last_name_column = [random.choice(apellidos_personas) for _ in range(num_rows)]

    # Columna 5: Party_Id (Número entero)
    party_id_column = [random.randint(1000000, 10000000) for _ in range(num_rows)]

    # Columna 6: Valor de moneda random entre 0 y 10 millones
    currency_column = [random.uniform(0, 10000000) for _ in range(num_rows)]

    pet_column = [random.choice(nombres_animales) for _ in range(num_rows)]

    dict_df = {
        'Fecha_Dt': date_column,
        'Log_Id': id_column,
        'Nombre_Tx': name_column,
        'Apellido_Tx': last_name_column,
        'Party_Id': party_id_column,
        'Fondos_Amt': currency_column,
        'Animal_Favorito_Tx': pet_column
    }

    for i in range(0, 8):
        random_column = [random.uniform(0, 10000) for _ in range(num_rows)]
        dict_df[f'Columna_{i}_Amt'] = random_column

    for i in range(8, 22):
        random_column = [random.randint(10000, 100000) for _ in range(num_rows)]
        dict_df[f'Columna_{i}_Nu'] = random_column

    # Crear DataFrame
    df = pd.DataFrame(dict_df)

    return df


def ask_user_pwd():
    """
        Esta función solicita al usuario un host, usuario y contraseña.
        :return: el host, usuario y contraseña
    """
    host = input(f'Ingrese host a conectarse: ')
    logmech = 'LDAP' if input(f'Debería usar LDAP para autenticar (s/n)?: ').strip().lower() == 's' else 'TD2'
    usr = input(f'Ingrese usuario de conexión: ')
    passw = getpass(f'Ingrese la contraseña para el usuario {usr}: ')
    return host, usr, passw, logmech

