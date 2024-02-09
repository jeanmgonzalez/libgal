# Teradata

## Interfaz simplificada para la carga de Dataframes a Teradata por Fastload y/o ODBC

### Descripción
Libgal define una interfaz simplificada para la carga de DataFrames a Teradata.

La carga se realiza por Fastload cuando la cantidad de registros es mayor a 10000, y por ODBC cuando es menor o igual a 10000.  
El parámetro de corte para utilizar un método u otro se puede modificar con el parámetro odbc_limit.

Para ver ejemplos de uso, ver la sección de [Ejemplos](by_example/Teradata_Examples.md).

## Importar la librería

```python
from libgal.modules.Teradata import TeradataML
```
---

## Índice
- [Conectarse al motor de base de datos y mantener la conexión abierta.](#teradatahost-str-user-str-passw-str-logmech-str--ldap-schema-str--none)
- [Ejecutar sentencias que no retornan datos (ej: create table, drop table, insert, update, delete, etc).](#doquery-str--executequery-str)
- [Ejecutar queries que retornan datos (ej: select) y devolver el resultado en un dataframe.](#queryquery-str-mode-str--normal---dataframe)
- [Truncar una tabla.](#truncate_tableschema-str-table-str)
- [Borrar una tabla.](#drop_tableschema-str-table-str)
- [Borrar una tabla si existe.](#drop_table_if_existsschema-str-table-str)
- [Obtener la lista de nombres de columnas de una tabla.](#table_columnsschema-str-table-str---liststr)
- [Obtener la lista de tablas de una base de datos que empiezan con un prefijo.](#show_tablesdb-str-prefix-str---dataframe)
- [Cargar un dataframe a una tabla.](#insertdf-dataframe-schema-str-table-str-pk-str-use_odbc-bool--true-odbc_limit-int--10000)
- [Actualizar forzado (upsert/insert overwrite) de un dataframe en una tabla.](#upsertdf-dataframe-schema-str-table-str-pk-str-use_odbc-bool--true-odbc_limit-int--10000-parser_limit-int--10000)
- [Crear una tabla que es copia de la estructura de otra.](#create_table_likeschema-str-table-str-schema_orig-str-table_orig-str)
- [Obtener la diferencia entre dos tablas.](#diffschema_src-str-table_src-str-schema_dst-str-table_dst-str---dataframe)
- [Realizar una carga incremental de un dataframe a una tabla.](#staging_insertdf-dataframe-schema_stg-str-table_stg-str-schema_dst-str-table_dst-str-pk-str)
- [Realizar un upsert incremental de un dataframe a una tabla.](#staging_upsertdf-dataframe-schema_stg-str-table_stg-str-schema_dst-str-table_dst-str-pk-str-parser_limit-int--10000)
- [Realizar un fastload de un dataframe a una tabla.](#fastloaddf-dataframe-schema-str-table-str-pk-str-index-bool--false)
- [Realizar un fastload con reintentos.](#retry_fastloaddf-dataframe-schema-str-table-str-pk-str-retries-int--30-retry_sleep-int--20)
- [Obtener la fecha desde el servidor (útil para test de conexión).](#current_date)
- [Cambiar la base de datos actual.](#use_dbdb-str)



## Teradata(host: str, user: str, passw: str, logmech: str = 'LDAP', schema: str = None)

Instancia el objeto y establece la conexión

Argumentos:
- host: Host de la base de datos
- user: Usuario
- passw: Contraseña
- logmech (opcional): Mecanismo de autenticación
- schema (opcional): Schema por defecto
- return: Objeto Teradata

**Ejemplo:**
```python
td = TeradataML(host='nombre_host', user='usuario', passw='contraseña')
```

Si se va a usar TD2 para la autenticación, se debe especificar el parámetro logmech='TD2'.  
En caso contrario, se puede omitir el parámetro logmech y por defecto será 'LDAP'.  
Opcionalmente se puede especificar el schema por defecto con el parámetro schema. 

---
## Métodos
### use_db(db: str)
Cambia la base de datos por defecto para las operaciones que se ejecuten a continuación.

Argumentos:
- db: Nombre de la base de datos

**Ejemplo:** 
```python
td.use_db('nombre_base_datos')
```

[Volver al inicio del documento](#Índice)

---
### do(query: str) / execute(query: str)
Ejecuta una sentencia sin retorno de datos. 

Argumentos:
- query: Query a ejecutar

**Ejemplo:**
```python
td.do('CREATE TABLE tabla (campo1 INT, campo2 VARCHAR(10))')
```

[Volver al inicio del documento](#Índice)

---
### query(query: str, mode: str = 'normal') -> DataFrame 
Ejecuta una query que devuelve un dataframe  

Argumentos:
- query: Query a ejecutar
- mode (opcional): Modo de ejecución, puede ser 'normal' o 'legacy'
- return: DataFrame con los resultados

**Ejemplo:**
```python
df = td.query('SELECT TOP 100 * FROM tabla')
```
Si se especifica el parámetro mode='legacy', utiliza el driver ODBC en vez de el engine de SQLAlchemy.  
Esto puede ser útil para ejecutar queries que no son soportadas por el engine de SQLAlchemy.  
Por lo general no es necesario especificar el modo.  

[Volver al inicio del documento](#Índice)

---
### current_date() 

Obtiene la fecha desde el servidor

Devuelve:  
return: datetime.date

**Ejemplo:**
```python
current_date = td.current_date()
```

[Volver al inicio del documento](#Índice)

---
### show_tables(db: str, prefix: str) -> DataFrame

Obtiene una lista de tablas de una base de datos que coinciden con prefijo + nombre_tabla

Argumentos:
- db: Base de datos
- prefix: Prefijo de la tabla  
- return: DataFrame con las tablas cuyo nombre empieza con el prefijo indicado

**Ejemplo:**
```python
tablas_df = td.show_tables(db='nombre_base_datos', prefix='prefijo_tabla')
```

[Volver al inicio del documento](#Índice)

---
### drop_table(schema: str, table: str)

Elimina una tabla

Argumentos:
- schema: Schema de la tabla
- table: Nombre de la tabla

**Ejemplo:**
```python
td.drop_table(schema='nombre_schema', table='nombre_tabla')
```
Ejecuta DROP TABLE nombre_schema.nombre_tabla;  
Si la tabla no existe, se produce una excepción teradatasql.OperationalError.  

[Volver al inicio del documento](#Índice)

---
### drop_table_if_exists(schema: str, table: str) 

Borra una tabla si existe.  

Argumentos:
- schema: Schema de la tabla
- table: Nombre de la tabla  

**Ejemplo:**
```python
td.drop_table_if_exists(schema='nombre_schema', table='nombre_tabla')
```
En el caso de que la tabla no exista, no se produce ningún error.

[Volver al inicio del documento](#Índice)

---
### truncate_table(schema: str, table: str)

Trunca una tabla (borra todos los registros pero no la estructura).  

Argumentos:
- schema: Schema de la tabla
- table: Nombre de la tabla

**Ejemplo:**
```python
td.truncate_table(schema='nombre_schema', table='nombre_tabla')
```
Ejecuta DELETE FROM nombre_schema.nombre_tabla ALL;

[Volver al inicio del documento](#Índice)

---
### table_columns(schema: str, table: str) -> List[str]  
Devuelve una lista con los nombres de las columnas de una tabla.  

Argumentos:
- schema: Schema de la tabla
- table: Nombre de la tabla

**Ejemplo:**
```python
columnas = td.table_columns(schema='nombre_schema', table='nombre_tabla')
```

[Volver al inicio del documento](#Índice)

---
### create_table_like(schema: str, table: str, schema_orig: str, table_orig: str) 
Crea una tabla que es copia de la estructura de otra

Argumentos:
- schema: Schema de la tabla a crear
- table: Nombre de la tabla a crear
- schema_orig: Schema de la tabla original / a copiar
- table_orig: Nombre de la tabla original / a copiar

**Ejemplo:**
```python
td.create_table_like(schema='nombre_schema', table='nombre_tabla', schema_orig='nombre_schema_orig', table_orig='nombre_tabla_orig')
```
Crea la tabla nombre_schema.nombre_tabla con la misma estructura que nombre_schema_orig.nombre_tabla_orig. 

[Volver al inicio del documento](#Índice)

---
### insert(df: DataFrame, schema: str, table: str, pk: str, use_odbc: bool = True, odbc_limit: int = 10000)
Inserta un dataframe en una tabla.  

Argumentos:
- df: DataFrame a insertar
- schema: Schema de la tabla
- table: Nombre de la tabla
- pk: Primary key de la tabla
- use_odbc (opcional): Usar ODBC para la inserción
- odbc_limit (opcional): Límite de filas para usar ODBC

**Ejemplo:**
```python
td.insert(df=df, schema='nombre_schema', table='nombre_tabla', pk='nombre_pk')
``` 
Inserta el dataframe df en la tabla nombre_schema.nombre_tabla.  
Si los registros existen, se produce una excepción teradatasql.IntegrityError.  
Al insertar menos de 10000 registros, se utiliza el driver ODBC, caso contrario se utiliza fastload.  
El límite de 10000 registros se puede modificar con el parámetro odbc_limit, y si se especifica use_odbc=False, se fuerza el uso de fastload. 

[Volver al inicio del documento](#Índice)

---
### upsert(df: DataFrame, schema: str, table: str, pk: str, use_odbc: bool = True, odbc_limit: int = 10000, parser_limit: int = 10000)

Actualiza un dataframe en una tabla forzado (insert overwrite/upsert)

Argumentos:
- df: DataFrame a insertar
- schema: Schema de la tabla
- table: Nombre de la tabla
- pk: Primary key de la tabla
- use_odbc (opcional): Usar ODBC para la inserción
- odbc_limit (opcional): Límite de filas para usar ODBC
- parser_limit (opcional): Límite de filas para el parser

Hace lo mismo que el método insert, pero si los registros existen, los actualiza.  

**Ejemplo:**
```python
td.upsert(df=df, schema='nombre_schema', table='nombre_tabla', pk='nombre_pk')
```
La actualización se realiza borrando los registros existentes y volviendo a insertarlos.  
El parámetro parser_limit se utiliza para dividir la cantidad de pks en grupos de tamaño parser_limit, y así evitar errores de parser al ejecutar el delete.  
Por lo general no es necesario modificar el parámetro parser_limit, pero si existen excepciones de parser, se puede probar con un valor mas bajo.

[Volver al inicio del documento](#Índice)

---
### fastload(df: DataFrame, schema: str, table: str, pk: str, index: bool = False)

Fastload de un dataframe a una tabla.

Argumentos:
- df: DataFrame a insertar
- schema: Schema de la tabla
- table: Nombre de la tabla
- pk: Primary key de la tabla
- index (opcional): Si se debe incluir el índice del dataframe

**Ejemplo:**
```python
td.fastload(df=df, schema='nombre_schema', table='nombre_tabla', pk='nombre_pk')
```

Inserta el dataframe df en la tabla nombre_schema.nombre_tabla utilizando fastload.  
Si los registros existen, se produce una excepción teradatasql.IntegrityError.

[Volver al inicio del documento](#Índice)

---
### retry_fastload(df: DataFrame, schema: str, table: str, pk: str, retries: int = 30, retry_sleep: int = 20)

Fastload con reintentos.  

Argumentos:
- df: DataFrame a insertar
- schema: Schema de la tabla
- table: Nombre de la tabla
- pk: Primary key de la tabla
- retries (opcional): Cantidad de reintentos
- retry_sleep (opcional): Tiempo de espera entre reintentos

Si se produce el error 2663 de fastload (Hay muchas instancias de fastload corriendo), se realiza un retry de la carga luego de esperar retry_sleep segundos.
Este método es equivalente a insert con el parámetro use_odbc=False.
Por defecto se realizan 30 reintentos con un tiempo de espera de 20 segundos entre reintentos.

**Ejemplo:**
```python
td.retry_fastload(df=df, schema='nombre_schema', table='nombre_tabla', pk='nombre_pk')
```

[Volver al inicio del documento](#Índice)

---
### diff(schema_src: str, table_src: str, schema_dst: str, table_dst: str) -> DataFrame

Devuelve un DataFrame con las diferencias entre dos tablas

Argumentos:
- schema_src: Schema de la tabla origen
- table_src: Nombre de la tabla origen
- schema_dst: Schema de la tabla destino
- table_dst: Nombre de la tabla destino  
- return: DataFrame con las diferencias

**Ejemplo:**
```python
df_diff = td.diff(schema_src='nombre_schema_src', table_src='nombre_tabla_src', schema_dst='nombre_schema_dst', table_dst='nombre_tabla_dst')
```  
Ejecuta SELECT * FROM nombre_schema_src.nombre_tabla_src MINUS SELECT * FROM nombre_schema_dst.nombre_tabla_dst; y devuelve el resultado en un dataframe.  
Este método se utiliza para la carga incremental de tablas que no tienen un primary key.  
Es recomendable de todas formas, que las tablas que vayan a realizar cargas incrementales tengan un primary key.  

[Volver al inicio del documento](#Índice)

---
### staging_insert(df: DataFrame, schema_stg: str, table_stg: str, schema_dst: str, table_dst: str, pk: str)

Realiza una carga incremental de un dataframe a una tabla.  
Primero se sube el lote a cargar a una tabla staging, y luego se realiza un insert desde ese staging a la tabla destino de todos los registros que no existen en la tabla destino.

Argumentos:
- df: DataFrame a insertar
- schema_stg: Schema de la tabla staging
- table_stg: Nombre de la tabla staging
- schema_dst: Schema de la tabla destino
- table_dst: Nombre de la tabla destino
- pk: Primary key de la tabla

**Ejemplo:**
```python
td.staging_insert(df=df, schema_stg='nombre_schema_stg', table_stg='nombre_tabla_stg', schema_dst='nombre_schema_dst', table_dst='nombre_tabla_dst', pk='nombre_pk')
```

[Volver al inicio del documento](#Índice)

---
### staging_upsert(df: DataFrame, schema_stg: str, table_stg: str, schema_dst: str, table_dst: str, pk: str, parser_limit: int = 10000):

Hace un upsert incremental de un dataframe a una tabla.  
La actualización se realiza borrando los registros existentes y volviendo a insertarlos al igual que en el método upsert.    
El parámetro parser_limit se utiliza de la misma forma que en el método upsert.

**Ejemplo:**
```python
td.staging_upsert(df=df, schema_stg='nombre_schema_stg', table_stg='nombre_tabla_stg', schema_dst='nombre_schema_dst', table_dst='nombre_tabla_dst', pk='nombre_pk')
```

[Volver al inicio del documento](#Índice)
