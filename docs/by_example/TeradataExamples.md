## Ejemplos de uso del módulo TeradataML de libgal

A continuación se presentan ejemplos de uso de la clase `TeradataML` para interactuar con Teradata.  
Los ejemplos están diseñados para ejecutarse en orden, por lo que se recomienda seguirlos en secuencia.

[Volver al readme principal](../../README.md)

## Índice
- [Inicializar el logger y conectar a Teradata](#ejemplo-0-inicializar-el-logger-y-conectar-a-teradata)  
- [Crear una tabla en Teradata](#ejemplo-1-crear-una-tabla-en-teradata)  
- [Insertar datos en una tabla en Teradata generando un primary key sintético](#ejemplo-2-insertar-datos-en-una-tabla-en-teradata-generando-un-primary-key-sintético)  
- [Consultar datos en una tabla en Teradata](#ejemplo-3-consultar-datos-en-una-tabla-en-teradata) 
- [Actualizar datos en una tabla en Teradata](#ejemplo-4-actualizar-datos-en-una-tabla-en-teradata) 
- [Eliminar una tabla en Teradata](#ejemplo-5-eliminar-una-tabla-en-teradata) 

### Ejemplo 0: Inicializar el logger y conectar a Teradata

```python
import os
from getpass import getpass
from libgal.modules.Teradata import TeradataML
from libgal.modules.Logger import Logger

# inicio el logger
logger = Logger().get_logger()

user = os.getlogin()
password = getpass(f"Ingrese la contraseña para el usuario {user}: ")
host = input("Ingrese el host de la base de datos: ")
logmech = input("Ingrese el logmech de la base de datos (LDAP por defecto): ")
schema = input("Ingrese el schema de la base de datos: ")

# creo una conexión a Teradata
td = TeradataML(host=host, user=user, passw=password, logmech=logmech, schema=schema)

# verifico que la conexión esté activa
if td.is_connected:
    logger.info("Conexión exitosa")
else:
    logger.error("No se pudo conectar a Teradata")
```

**Salida:**

```
2024-02-09 11:22:42,097 PID: 944 (1429630139984) MainThread [INFO | Logger.py:80] > Generate new instance, hash = 1429630139984
Ingrese la contraseña para el usuario l*******:
Ingrese el host de la base de datos: *****
Ingrese el logmech de la base de datos (LDAP por defecto): LDAP
Ingrese el schema de la base de datos: p_staging
2024-02-09 11:23:00,024 PID: 944 (1429630139984) MainThread [INFO | Teradata.py:184] > Conectando TeradataML
2024-02-09 11:23:11,206 PID: 944 (1429630139984) MainThread [INFO | scratch.py:20] > Conexión exitosa
```

[Volver al índice](#índice)

### Ejemplo 1: Crear una tabla en Teradata 

```python
import teradatasql

create_stmt = '''
    CREATE TABLE p_staging.teradataml_test_table (
        hash_key VARCHAR(100),
        boletin VARCHAR(100),
        empresa VARCHAR(100),
        nombre VARCHAR(100),
        apellido VARCHAR(100),
        nro_documento BIGINT,
        fecha TIMESTAMP(6)
    ) UNIQUE PRIMARY INDEX (hash_key)
'''

try:
    td.do(create_stmt)
except teradatasql.OperationalError as e:
    logger.warning(f'La tabla ya existe: p_staging.teradataml_test_table')
```

**Salida:**

```
2024-02-09 11:30:35,057 PID: 15516 (2481885527824) MainThread [DEBUG | Teradata.py:228] > Ejecutando query:
    CREATE TABLE p_staging.teradataml_test_table (
        hash_key VARCHAR(100),
        boletin VARCHAR(100),
        empresa VARCHAR(100),
        nombre VARCHAR(100),
        apellido VARCHAR(100),
        nro_documento BIGINT,
        fecha TIMESTAMP(6)
    ) PRIMARY INDEX (hash_key)
```

Si la tabla existe, se mostrará el siguiente mensaje:

```
2024-02-09 11:32:32,145 PID: 8988 (2176916963088) MainThread [WARNING | scratch_2.py:40] > La tabla ya existe: p_staging.teradataml_test_table
```

[Volver al índice](#índice)

### Ejemplo 2: Insertar datos en una tabla en Teradata generando un primary key sintético

```python
import pandas as pd
from libgal.modules.Utils import hash_primary_key
from datetime import datetime

# preparo un dataframe de ejemplo
df = pd.DataFrame([
    {
        "boletin": 'nacional',
        "empresa": 'afe',
        "nombre": 'juan',
        "apellido": 'perez',
        "nro_documento": '123',
        "fecha": "2024-01-01"
    },
    {
        "boletin": 'provincial',
        "empresa": 'alicia',
        "nombre": 'maria',
        "apellido": 'gomez',
        "nro_documento": '1114578',
        "fecha": "2024-01-02"
    }
])

schema = 'p_staging'
# genero una clave única para cada registro
df['hash_key'] = df.apply(lambda row: hash_primary_key(row, ['boletin', 'empresa', 'nro_documento'], 'fecha', trim=10), axis=1)

# convierto las fechas a datetime para evitar error de invalid timestamp
df['fecha'] = df.apply(lambda row: datetime.strptime(row['fecha'], '%Y-%m-%d'), axis=1)

# inserto el dataframe en una tabla de la base de datos
td.insert(df, schema, 'teradataml_test_table', pk='hash_key')

```

**Salida**
```
2024-02-09 12:49:47,133 PID: 1456 (1918861638720) MainThread [INFO | Teradata.py:328] > Cargando lote 1 de 1
```

[Volver al índice](#índice)

### Ejemplo 3: Consultar datos en una tabla en Teradata

```python
from tabulate import tabulate # pip install tabulate

data_read = td.query(f"SEL * FROM p_staging.teradataml_test_table ORDER BY hash_key")

# dataframe original
print("Dataframe original:")
print(tabulate(df, headers='keys', tablefmt='psql'))
# dataframe resultante
print("Dataframe leído:")
print(tabulate(data_read, headers='keys', tablefmt='psql'))

```

**Salida**
```
2024-02-09 13:08:03,165 PID: 1580 (2541863304112) MainThread [DEBUG | Teradata.py:241] > Ejecutando query: SEL * FROM p_staging.teradataml_test_table ORDER BY
hash_key
Dataframe original:
+----+------------+-----------+----------+------------+-----------------+---------------------+-----------------------+
|    | boletin    | empresa   | nombre   | apellido   |   nro_documento | fecha               | hash_key              |
|----+------------+-----------+----------+------------+-----------------+---------------------+-----------------------|
|  0 | nacional   | afe       | juan     | perez      |             123 | 2024-01-01 00:00:00 | 1704078000_e59823794c |
|  1 | provincial | alicia    | maria    | gomez      |         1114578 | 2024-01-02 00:00:00 | 1704164400_31cab20123 |
+----+------------+-----------+----------+------------+-----------------+---------------------+-----------------------+
Dataframe leído:
+----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------+
|    | hash_key              | boletin    | empresa   | nombre   | apellido   |   nro_documento | fecha               |
|----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------|
|  0 | 1704078000_e59823794c | nacional   | afe       | juan     | perez      |             123 | 2024-01-01 00:00:00 |
|  1 | 1704164400_31cab20123 | provincial | alicia    | maria    | gomez      |         1114578 | 2024-01-02 00:00:00 |
+----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------+
```

[Volver al índice](#índice)

### Ejemplo 4: Actualizar datos en una tabla en Teradata

#### Utilizando el método `upsert`

```python
# preparo una copia de data_read con variaciones en los campos que no conforman el primary_key
df_update = data_read.copy()
df_update.loc[(df_update['nombre'] == 'juan') & (df_update['nro_documento'] == 123), 'nombre'] = 'Juan'
df_update.loc[(df_update['apellido'] == 'perez') & (df_update['nro_documento'] == 123), 'apellido'] = 'Pérez'
df_update.loc[(df_update['nombre'] == 'maria') & (df_update['nro_documento'] == 1114578), 'nombre'] = 'María'
df_update.loc[(df_update['apellido'] == 'gomez') & (df_update['nro_documento'] == 1114578), 'apellido'] = 'Gómez'


# regenero el hash_key (tiene que devolver los mismos hash keys porque no cambia ni boletin, ni empresa ni número de documento
df_update['hash_key'] = df_update.apply(
    lambda row: hash_primary_key(
        row,
        ['boletin', 'empresa', 'nro_documento'],
        'fecha',
        timestamp_format='iso',
        trim=10)
    , axis=1
)

# actualizo los registros de la tabla
td.upsert(df_update, schema, 'teradataml_test_table', pk='hash_key')

data_read = td.query(f"SEL * FROM p_staging.teradataml_test_table ORDER BY hash_key")

# dataframe original
print("Dataframe original:")
print(tabulate(df, headers='keys', tablefmt='psql'))
# dataframe actualizado
print("Dataframe actualizado:")
print(tabulate(df_update, headers='keys', tablefmt='psql'))
# dataframe resultante
print("Dataframe leído:")
print(tabulate(data_read, headers='keys', tablefmt='psql'))
```

**Salida**
```
2024-02-09 14:26:10,827 PID: 17324 (1460784937856) MainThread [DEBUG | Teradata.py:228] > Ejecutando query: DELETE FROM p_staging.teradataml_test_table WHERE h
ash_key IN ('1704078000_e59823794c','1704164400_31cab20123');
2024-02-09 14:26:10,861 PID: 17324 (1460784937856) MainThread [INFO | Teradata.py:328] > Cargando lote 1 de 1
2024-02-09 14:26:11,042 PID: 17324 (1460784937856) MainThread [DEBUG | Teradata.py:241] > Ejecutando query: SEL * FROM p_staging.teradataml_test_table ORDER BY
 hash_key
Dataframe original:
+----+------------+-----------+----------+------------+-----------------+---------------------+-----------------------+
|    | boletin    | empresa   | nombre   | apellido   |   nro_documento | fecha               | hash_key              |
|----+------------+-----------+----------+------------+-----------------+---------------------+-----------------------|
|  0 | nacional   | afe       | juan     | perez      |             123 | 2024-01-01 00:00:00 | 1704078000_e59823794c |
|  1 | provincial | alicia    | maria    | gomez      |         1114578 | 2024-01-02 00:00:00 | 1704164400_31cab20123 |
+----+------------+-----------+----------+------------+-----------------+---------------------+-----------------------+
Dataframe actualizado:
+----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------+
|    | hash_key              | boletin    | empresa   | nombre   | apellido   |   nro_documento | fecha               |
|----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------|
|  0 | 1704078000_e59823794c | nacional   | afe       | Juan     | Pérez      |             123 | 2024-01-01 00:00:00 |
|  1 | 1704164400_31cab20123 | provincial | alicia    | María    | Gómez      |         1114578 | 2024-01-02 00:00:00 |
+----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------+
Dataframe leído:
+----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------+
|    | hash_key              | boletin    | empresa   | nombre   | apellido   |   nro_documento | fecha               |
|----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------|
|  0 | 1704078000_e59823794c | nacional   | afe       | Juan     | Pérez      |             123 | 2024-01-01 00:00:00 |
|  1 | 1704164400_31cab20123 | provincial | alicia    | María    | Gómez      |         1114578 | 2024-01-02 00:00:00 |
+----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------+
```

[Volver al índice](#índice)

#### Creando una sentencia `update` de Teradata

**Nota**: no se regenera el hash_key porque no cambia ni boletin, ni empresa ni número de documento

```python
# obtengo el hash_key de un registro por su nombre y nro_documento
hash_key_juan = df.loc[(df['nombre'] == 'juan') & (df['nro_documento'] == '123')]['hash_key'].values[0]
hash_key_maria = df.loc[(df['nombre'] == 'maria') & (df['nro_documento'] == '1114578')]['hash_key'].values[0]

# preparo la sentencia de update seteando Juan en nombre y Gómez en apellido
update_stmt = f'''
    UPDATE {schema}.teradataml_test_table
    SET nombre = 'Juan', apellido = 'Gómez'
    WHERE hash_key = '{hash_key_juan}';
'''
# hago lo mismo con maria, pero seteando María en nombre y Pérez en apellido
update_stmt += f'''
    UPDATE {schema}.teradataml_test_table
    SET nombre = 'María', apellido = 'Pérez'
    WHERE hash_key = '{hash_key_maria}';
'''

# ejecuto la sentencia de update
td.do(update_stmt)

data_read2 = td.query(f"SEL * FROM p_staging.teradataml_test_table ORDER BY hash_key")

# dataframe original
print("Dataframe antes del update:")
print(tabulate(data_read, headers='keys', tablefmt='psql'))
# dataframe resultante
print("Dataframe luego del update:")
print(tabulate(data_read2, headers='keys', tablefmt='psql'))
```

**Salida**
```
2024-02-09 14:44:18,357 PID: 14648 (2754324777952) MainThread [DEBUG | Teradata.py:228] > Ejecutando query:
    UPDATE p_staging.teradataml_test_table
    SET nombre = 'Juan', apellido = 'Gómez'
    WHERE hash_key = '1704078000_e59823794c';

    UPDATE p_staging.teradataml_test_table
    SET nombre = 'María', apellido = 'Pérez'
    WHERE hash_key = '1704164400_31cab20123';

2024-02-09 14:44:18,412 PID: 14648 (2754324777952) MainThread [DEBUG | Teradata.py:241] > Ejecutando query: SEL * FROM p_staging.teradataml_test_table ORDE
R BY hash_key
Dataframe antes del update:
+----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------+
|    | hash_key              | boletin    | empresa   | nombre   | apellido   |   nro_documento | fecha               |
|----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------|
|  0 | 1704078000_e59823794c | nacional   | afe       | juan     | perez      |             123 | 2024-01-01 00:00:00 |
|  1 | 1704164400_31cab20123 | provincial | alicia    | maria    | gomez      |         1114578 | 2024-01-02 00:00:00 |
+----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------+
Dataframe luego del update:
+----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------+
|    | hash_key              | boletin    | empresa   | nombre   | apellido   |   nro_documento | fecha               |
|----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------|
|  0 | 1704078000_e59823794c | nacional   | afe       | Juan     | Gómez      |             123 | 2024-01-01 00:00:00 |
|  1 | 1704164400_31cab20123 | provincial | alicia    | María    | Pérez      |         1114578 | 2024-01-02 00:00:00 |
+----+-----------------------+------------+-----------+----------+------------+-----------------+---------------------+
```

[Volver al índice](#índice)

### Ejemplo 5: Eliminar una tabla en Teradata

```python
# elimino la tabla
td.drop_table(schema, 'teradataml_test_table')
```

[Volver al índice](#índice)

Para más información sobre el módulo TeradataML, puede consultar la documentación en [este enlace](../Teradata.md).

