### Módulo de funciones auxiliares y utilidades

En este módulo se encuentran funciones auxiliares de libgal.

[Volver al readme principal](../README.md)

### Importar

```python
from libgal.modules.Utils import drop_lists, chunks, chunks_df, remove_non_latin1, powercenter_compat_df, powercenter_compat_str, hash_primary_key
``` 


### Funciones

- [`drop_lists`](#drop_lists): Elimina las celdas con listas del dataframe.
- [`chunks`](#chunks): Divide una lista en partes de tamaño `n`.
- [`chunks_df`](#chunks_df): Divide un DataFrame en partes de tamaño `n`.
- [`remove_non_latin1`](#remove_non_latin1): Elimina caracteres no latinos de un string.
- [`powercenter_compat_df`](#powercenter_compat_df): Ajusta un DataFrame para ser compatible con PowerCenter.
- [`powercenter_compat_str`](#powercenter_compat_str): Ajusta un string para ser compatible con PowerCenter.
- [`hash_primary_key`](#hash_primary_key): Genera un hash de una clave primaria.


## drop_lists

Esta función elimina las celdas con listas del dataframe.

**Ejemplo:**
```python
import pandas as pd
from libgal.modules.Utils import drop_lists

df = pd.DataFrame({'name': 'non_list_value', 'a': [1, 2, 3], 'b': [[1, 2], [3, 4], [5, 6]], 'c': 123})
df = drop_lists(df)
print(df)
```
**Salida:**
```
             name  a    c
0  non_list_value  1  123
1  non_list_value  2  123
2  non_list_value  3  123
```
Se utiliza en los casos donde se requiere hacer un flatten de un DataFrame.
La operación flatten consiste en convertir un DataFrame anidado en un DataFrame plano.

[Volver a inicio del documento](#funciones)

## chunks

Se utiliza para dividir una lista en partes de tamaño `n`. 

**Ejemplo:**
```python
from libgal.modules.Utils import chunks

lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(list(chunks(lista, 3)))
```
**Salida:**
```
[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]
```

[Volver a inicio del documento](#funciones)

## chunks_df

Se utiliza para dividir un DataFrame en partes de tamaño `n`. 

**Ejemplo:**
```python
import pandas as pd
from libgal.modules.Utils import chunks_df
from tabulate import tabulate # no incluido en libgal, se debe instalar con pip

df = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 'b': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})

print('Original DataFrame')
print(tabulate(df, headers='keys', tablefmt='psql'))
chunks = chunks_df(df, 3)
for i, chunk in enumerate(chunks):
    print(f'Chunk {i + 1}')
    print(tabulate(chunk, headers='keys', tablefmt='psql'))

```
**Salida:**
```
Original DataFrame
+----+-----+-----+
|    |   a |   b |
|----+-----+-----|
|  0 |   1 |   1 |
|  1 |   2 |   2 |
|  2 |   3 |   3 |
|  3 |   4 |   4 |
|  4 |   5 |   5 |
|  5 |   6 |   6 |
|  6 |   7 |   7 |
|  7 |   8 |   8 |
|  8 |   9 |   9 |
|  9 |  10 |  10 |
+----+-----+-----+
Chunk 1
+----+-----+-----+
|    |   a |   b |
|----+-----+-----|
|  0 |   1 |   1 |
|  1 |   2 |   2 |
|  2 |   3 |   3 |
|  3 |   4 |   4 |
+----+-----+-----+
Chunk 2
+----+-----+-----+
|    |   a |   b |
|----+-----+-----|
|  4 |   5 |   5 |
|  5 |   6 |   6 |
|  6 |   7 |   7 |
+----+-----+-----+
Chunk 3
+----+-----+-----+
|    |   a |   b |
|----+-----+-----|
|  7 |   8 |   8 |
|  8 |   9 |   9 |
|  9 |  10 |  10 |
+----+-----+-----+
```
Esta función es útil para dividir un DataFrame en partes más pequeñas para su procesamiento individual o para
distruibuir el procesamiento en paralelo.

[Volver a inicio del documento](#funciones)

## remove_non_latin1

Esta función elimina caracteres no latin1 de un string.

**Ejemplo:**
```python
from libgal.modules.Utils import remove_non_latin1

string = 'áéíóúñçü, ÁÉÍÓÚÑÇÜ, 1234567890, !@#$%^&*()_+-=[]{};:<>¿?/.\\'

# agrego caracteres no latin1
string += '👍👎👌👏👋👊🤝🤜🤛🤞🤟🤘🤙🤚🤛🤜🤝🤞🤟'

print('String con caracteres no latin1:', string)
print('String sin caracteres no latin1:', remove_non_latin1(string))
```
**Salida:**
```
String con caracteres no latin1: áéíóúñçü, ÁÉÍÓÚÑÇÜ, 1234567890, !@#$%^&*()_+-=[]{};:<>¿?/.\👍👎👌👏👋👊🤝🤜🤛🤞🤟🤘🤙🤚🤛🤜🤝🤞🤟
String sin caracteres no latin1: áéíóúñçü, ÁÉÍÓÚÑÇÜ, 1234567890, !@#$%^&*()_+-=[]{};:<>¿?/.\
```

[Volver a inicio del documento](#funciones)

## powercenter_compat_str

Esta función ajusta un string para ser compatible con FlatFile de PowerCenter.

**Ejemplo:**
```python
from libgal.modules.Utils import powercenter_compat_str

# string con caracteres de control como retorno de carro, tabs, pipes, etc.
string = '\ra\nb\tc d|e'
print('String original:\n', string)
print('String compatible con PowerCenter:', powercenter_compat_str(string))
```
**Salida:**
```
String original:
a
b	c d|e
String compatible con PowerCenter:  a b c d e
```

[Volver a inicio del documento](#funciones)

## powercenter_compat_df

Esta función ajusta un DataFrame para ser compatible con FlatFile de PowerCenter cuando se exporte a .csv.

**Ejemplo:**
```python
import pandas as pd
from libgal.modules.Utils import powercenter_compat_df
from tabulate import tabulate # no incluido en libgal, se debe instalar con pip

df = pd.DataFrame({'a': ['a', 'b\r\n', 'c|'], 'b': ['d', '\\r\\n\te', 'f'], 'c': ['g', 'h', 'i']})
print('DataFrame original:')
print(tabulate(df, headers='keys', tablefmt='psql'))
print('DataFrame compatible con PowerCenter:')
print(tabulate(powercenter_compat_df(df), headers='keys', tablefmt='psql'))
```
**Salida:**
```
DataFrame original:
+----+-----+--------+-----+
|    | a   | b      | c   |
|----+-----+--------+-----|
|  0 | a   | d      | g   |
|  1 | b   | \r\n	e | h   |
|  2 | c|  | f      | i   |
+----+-----+--------+-----+
DataFrame compatible con PowerCenter:
+----+-----+-----+-----+
|    | a   | b   | c   |
|----+-----+-----+-----|
|  0 | a   | d   | g   |
|  1 | b   | e   | h   |
|  2 | c   | f   | i   |
+----+-----+-----+-----+
```

[Volver a inicio del documento](#funciones)

## hash_primary_key

Esta función genera un hash de una fila/registro de un DataFrame para ser utilizado como clave primaria.  
Se puede especificar el nombre de las columnas que se utilizarán para generar el hash y opcionalmente un campo de fecha.  
Si se utiliza un campo de fecha, se debe especificar el formato de la fecha, por defecto es `'%Y-%m-%d'`.

**Ejemplo:**
```python
import pandas as pd
from libgal.modules.Utils import hash_primary_key
from tabulate import tabulate  # no incluido en libgal, se debe instalar con pip


def hash_key(row: dict) -> str:
    """
        Esta función la implementará el desarrollador en para generar la clave única de un registro.
        Definirá los campos que se usarán para generar la clave y el formato de fecha y hora.
    """
    return hash_primary_key(row, ['boletin', 'empresa', 'fecha', 'nro_documento'], 'fecha')


df = pd.DataFrame([
    {
        "boletin": 'nacional',
        "empresa": 'cfe',
        "fecha": '2021-01-01',
        "nro_documento": '123456'
    },
    {
        "boletin": 'provincial',
        "empresa": 'alicia',
        "fecha": '2021-01-02',
        "nro_documento": '1114578'
    }
])

print('DataFrame original:')
print(tabulate(df, headers='keys', tablefmt='psql'))
df['hash_key'] = df.apply(hash_key, axis=1)
print('DataFrame con hash:')
print(tabulate(df, headers='keys', tablefmt='psql'))
```
**Salida:**
```
DataFrame original:
+----+------------+-----------+------------+-----------------+
|    | boletin    | empresa   | fecha      |   nro_documento |
|----+------------+-----------+------------+-----------------|
|  0 | nacional   | cfe       | 2021-01-01 |          123456 |
|  1 | provincial | alicia    | 2021-01-02 |         1114578 |
+----+------------+-----------+------------+-----------------+
DataFrame con hash:
+----+------------+-----------+------------+-----------------+-------------------------+
|    | boletin    | empresa   | fecha      |   nro_documento | hash_key                |
|----+------------+-----------+------------+-----------------+-------------------------|
|  0 | nacional   | cfe       | 2021-01-01 |          123456 | 1609470000_6eefe34fd748 |
|  1 | provincial | alicia    | 2021-01-02 |         1114578 | 1609556400_d2cc9ec83c94 |
+----+------------+-----------+------------+-----------------+-------------------------+
```
La función `hash_key` es un ejemplo de cómo se puede implementar la función `hash_primary_key` para generar una clave única para un registro.
Los mismos datos siempre generarán el mismo hash, por lo que se puede utilizar para identificar un registro de manera única.

En el caso que el registro no contenga fecha, se puede utilizar la función `hash_primary_key` sin el campo de fecha.

**Ejemplo:**
```python
import pandas as pd
from libgal.modules.Utils import hash_primary_key
from tabulate import tabulate  # no incluido en libgal, se debe instalar con pip

df = pd.DataFrame([
    {
        "boletin": 'nacional',
        "empresa": 'cfe',
        "nombre": 'juan',
        "apellido": 'perez',
        "nro_documento": '123456'

    },
    {
        "boletin": 'provincial',
        "empresa": 'alicia',
        "nombre": 'maria',
        "apellido": 'gomez',
        "nro_documento": '1114578'
    }
])

print('DataFrame original:')
print(tabulate(df, headers='keys', tablefmt='psql'))
df['hash_key'] = df.apply(lambda row: hash_primary_key(row, ['boletin', 'empresa', 'nro_documento']), axis=1)
print('DataFrame con hash:')
print(tabulate(df, headers='keys', tablefmt='psql'))
```

**Salida:**
```
DataFrame original:
+----+------------+-----------+----------+------------+-----------------+
|    | boletin    | empresa   | nombre   | apellido   |   nro_documento |
|----+------------+-----------+----------+------------+-----------------|
|  0 | nacional   | cfe       | juan     | perez      |          123456 |
|  1 | provincial | alicia    | maria    | gomez      |         1114578 |
+----+------------+-----------+----------+------------+-----------------+
DataFrame con hash:
+----+------------+-----------+----------+------------+-----------------+------------------------------------------------------------------+
|    | boletin    | empresa   | nombre   | apellido   |   nro_documento | hash_key                                                         |
|----+------------+-----------+----------+------------+-----------------+------------------------------------------------------------------|
|  0 | nacional   | cfe       | juan     | perez      |          123456 | da127fef46891bd9200cd0ead0ee4c2b6371bc898eb227aae72be706dd579a70 |
|  1 | provincial | alicia    | maria    | gomez      |         1114578 | 31cab20123ae91980df22b26957d97c84abb35bef75f3a97d3891efa89f27a94 |
+----+------------+-----------+----------+------------+-----------------+------------------------------------------------------------------+
```

En el caso que se quiera truncar el hash a un tamaño específico, se puede utilizar la función `hash_primary_key` con el parámetro `trim`:

**Ejemplo:**
```python
import pandas as pd
from libgal.modules.Utils import hash_primary_key
from tabulate import tabulate  # no incluido en libgal, se debe instalar con pip

df = pd.DataFrame([
    {
        "boletin": 'nacional',
        "empresa": 'cfe',
        "nombre": 'juan',
        "apellido": 'perez',
        "nro_documento": '123'
    },
    {
        "boletin": 'provincial',
        "empresa": 'alicia',
        "nombre": 'maria',
        "apellido": 'gomez',
        "nro_documento": '1114578'
    }
])

print('DataFrame original:')
print(tabulate(df, headers='keys', tablefmt='psql'))
df['hash_key'] = df.apply(lambda row: hash_primary_key(row, ['boletin', 'empresa', 'nro_documento'], trim=10), axis=1)
print('DataFrame con hash:')
print(tabulate(df, headers='keys', tablefmt='psql'))
```

**Salida:**
```
DataFrame original:
+----+------------+-----------+----------+------------+-----------------+
|    | boletin    | empresa   | nombre   | apellido   |   nro_documento |
|----+------------+-----------+----------+------------+-----------------|
|  0 | nacional   | cfe       | juan     | perez      |             123 |
|  1 | provincial | alicia    | maria    | gomez      |         1114578 |
+----+------------+-----------+----------+------------+-----------------+
DataFrame con hash:
+----+------------+-----------+----------+------------+-----------------+------------+
|    | boletin    | empresa   | nombre   | apellido   |   nro_documento | hash_key   |
|----+------------+-----------+----------+------------+-----------------+------------|
|  0 | nacional   | cfe       | juan     | perez      |             123 | 8216fbafde |
|  1 | provincial | alicia    | maria    | gomez      |         1114578 | 31cab20123 |
+----+------------+-----------+----------+------------+-----------------+------------+
```
Al momento de truncar el hash, se debe tener en cuenta que se puede generar colisiones, por lo que se debe elegir un tamaño de hash que minimice la probabilidad de colisiones.

[Volver a inicio del documento](#funciones)