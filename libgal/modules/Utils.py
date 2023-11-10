import string
from typing import Optional, List
from pandas import DataFrame
import numpy as np


# Elimina las celdas con listas del dataframe
def drop_lists(df: DataFrame):
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


# divide una lista en porciones de tamaño n
def chunks(lst: list, n: int) -> list:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# lo mismo pero con dataframes
def chunks_df(df: DataFrame, n: int) -> List[DataFrame]:
    chunk_size = max(int(len(df) / n), 1)
    return np.array_split(df, chunk_size)


# filtra los caracteres no latin1 del input str
def remove_non_latin1(a_str: Optional[str]) -> Optional[str]:
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


# devuelve una representación de string compatible con FlatFile de PWC
def powercenter_compat_msg(message: DataFrame) -> DataFrame:
    return message.replace(
        to_replace=[r"\\t|\\n|\\r|\|", "\t|\n|\r"],
        value=[' ', ' '],
        regex=True
    )


# lo mismo que el anterior, pero sobre strings
def powercenter_compat_str(message: str) -> str:
    return message.translate(
        str.maketrans({
            '\\t': '  ', '\\n': ' ', '\\r': ' ', '|': ' ',
            '\t': '  ', '\n': ' ', '\r': ' '
        })
    )

