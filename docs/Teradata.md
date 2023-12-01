# Teradata

## Interfaz simplificada para la carga de Dataframes a Teradata por Fastload y/o ODBC

### Descripción
Libgal define una interfaz simplificada para la carga de DataFrames a Teradata.

La carga se realiza por Fastload cuando la cantidad de registros es mayor a 10000, y por ODBC cuando es menor o igual a 10000.  
El parámetro de corte para utilizar un método u otro se puede modificar con el parámetro odbc_limit.

## Importar la librería
```python
from libgal.modules.Teradata import Teradata
```
---

**Índice de características y funciones**
- [Conectarse al motor de base de datos y mantener la conexión abierta.](#instanciar-el-objeto-y-establecer-la-conexión)
- [Ejecutar sentencias que no retornan datos (ej: create table, drop table, insert, update, delete, etc).](#ejecutar-una-sentencia-sin-retorno-de-datos)
- [Ejecutar queries que retornan datos (ej: select) y devolver el resultado en un dataframe.](#ejecutar-una-query-que-devuelve-un-dataframe)
- [Truncar una tabla.](#truncar-una-tabla)
- [Borrar una tabla.](#eliminar--dropear-una-tabla)
- [Borrar una tabla si existe.](#borrar-una-tabla-si-existe)
- [Obtener la lista de nombres de columnas de una tabla.](#obtener-los-nombres-de-las-columnas-de-una-tabla)
- [Obtener la lista de tablas de una base de datos que empiezan con un prefijo.](#obtener-una-lista-de-tablas-de-una-base-de-datos-que-coinciden-con-prefijo--nombre_tabla)
- [Cargar un dataframe a una tabla.](#insertar-un-dataframe-en-una-tabla)
- [Actualizar forzado (upsert/insert overwrite) de un dataframe en una tabla.](#actualizar-un-dataframe-en-una-tabla-forzado-insert-overwriteupsert)
- [Crear una tabla que es copia de la estructura de otra.](#crear-una-tabla-que-es-copia-de-la-estructura-de-otra)
- [Obtener la diferencia entre dos tablas.](#obtener-la-diferencia-entre-dos-tablas)
- [Realizar una carga incremental de un dataframe a una tabla.](#carga-incremental-de-un-dataframe-a-una-tabla)
- [Realizar un upsert incremental de un dataframe a una tabla.](#upsert-incremental-de-un-dataframe-a-una-tabla)
- [Realizar un fastload de un dataframe a una tabla.](#fastload-de-un-dataframe-a-una-tabla)
- [Realizar un fastload con reintentos.](#fastload-con-reintentos)
- [Obtener la fecha desde el servidor (útil para test de conexión).](#obtener-la-fecha-desde-el-servidor)
- [Cambiar la base de datos actual.](#cambiar-la-base-de-datos-actual)


## Instanciar el objeto y establecer la conexión
```python
"""
Inicializa una conexión a Teradata
    :param host: Host de la base de datos
    :param user: Usuario
    :param passw: Contraseña
    :param logmech: Mecanismo de autenticación
    :param schema: Schema por defecto
"""
td = Teradata(host='nombre_host', user='usuario', passw='contraseña')
```

Si se va a usar LDAP para la autenticación, se debe especificar el parámetro logmech='LDAP'.  
En caso contrario, se puede omitir el parámetro logmech y por defecto será 'TD2'.  
Opcionalmente se puede especificar el schema por defecto con el parámetro schema. 

---
## Funciones
### Cambiar la base de datos actual
```python
    def use_db(self, db: str):
        """
        Cambia la base de datos por defecto
            :param db: Nombre de la base de datos
        """
```
Cambia la base de datos por defecto para las operaciones que se ejecuten a continuación.
**Ejemplo:** 
```python
td.use_db('nombre_base_datos')
```
---
### Ejecutar una sentencia sin retorno de datos
```python
    def do(self, query: str):
        """
        Ejecuta una query que no devuelve resultados
            :param query: Query a ejecutar
        """
```
**Ejemplo:**
```python
td.do('CREATE TABLE tabla (campo1 INT, campo2 VARCHAR(10))')
```
---
### Ejecutar una query que devuelve un dataframe
```python
    def query(self, query: str, mode: str = 'normal') -> DataFrame:
        """
        Ejecuta una query que devuelve resultados
            :param query: Query a ejecutar
            :param mode: Modo de ejecución, puede ser 'normal' o 'legacy'
            :return: DataFrame con los resultados
        """
```
**Ejemplo:**
```python
df = td.query('SELECT TOP 100 * FROM tabla')
```
Si se especifica el parámetro mode='legacy', utiliza el driver ODBC en vez de el engine de SQLAlchemy.  
Esto puede ser útil para ejecutar queries que no son soportadas por el engine de SQLAlchemy.  
Por lo general no es necesario especificar el modo.  

---
### Obtener la fecha desde el servidor
```python
    def current_date(self) -> datetime.date:
        """
        Devuelve la fecha del servidor de la base de datos
        """
```
**Ejemplo:**
```python
current_date = td.current_date()
```

---
### Obtener una lista de tablas de una base de datos que coinciden con prefijo + nombre_tabla
```python
    def show_tables(self, db: str, prefix: str) -> DataFrame:
        """
        Devuelve un DataFrame con las tablas que empiezan con un prefijo
            :param db: Base de datos
            :param prefix: Prefijo de la tabla
            :return: DataFrame con las tablas que empiezan con el prefijo
        """
```
**Ejemplo:**
```python
tablas_df = td.show_tables(db='nombre_base_datos', prefix='prefijo_tabla')
```
---
### Eliminar / dropear una tabla
```python
    def drop_table(self, schema: str, table: str):
        """
        Elimina una tabla
            :param schema: Schema de la tabla
            :param table: Nombre de la tabla
        """
```
**Ejemplo:**
```python
td.drop_table(schema='nombre_schema', table='nombre_tabla')
```
Ejecuta DROP TABLE nombre_schema.nombre_tabla;  
Si la tabla no existe, se produce una excepción teradatasql.OperationalError.  

---
### Borrar una tabla si existe
```python
    def drop_table_if_exists(self, schema: str, table: str):
        """
        Elimina una tabla si existe
            :param schema: Schema de la tabla
            :param table: Nombre de la tabla
        """
```
**Ejemplo:**
```python
td.drop_table_if_exists(schema='nombre_schema', table='nombre_tabla')
```
En el caso de que la tabla no exista, no se produce ningún error.

---
### Truncar una tabla
```python
    def truncate_table(self, schema: str, table: str):
        """
        Trunca una tabla (borra todos los registros pero no la estructura)
            :param schema: Schema de la tabla
            :param table: Nombre de la tabla
        """
```
**Ejemplo:**
```python
td.truncate_table(schema='nombre_schema', table='nombre_tabla')
```
Ejecuta DELETE FROM nombre_schema.nombre_tabla ALL;

---
### Obtener los nombres de las columnas de una tabla
```python
    def table_columns(self, schema: str, table: str) -> List[str]:
        """
        Devuelve una lista con los nombres de las columnas de una tabla
            :param schema: Schema de la tabla
            :param table: Nombre de la tabla
        """
```
**Ejemplo:**
```python
columnas = td.table_columns(schema='nombre_schema', table='nombre_tabla')
```

---
### Crear una tabla que es copia de la estructura de otra
```python
    def create_table_like(self, schema: str, table: str, schema_orig: str, table_orig: str):
        """
        Crea una tabla con la misma estructura que otra
            :param schema: Schema de la tabla a crear
            :param table: Nombre de la tabla a crear
            :param schema_orig: Schema de la tabla original
            :param table_orig: Nombre de la tabla original
        """
```
**Ejemplo:**
```python
td.create_table_like(schema='nombre_schema', table='nombre_tabla', schema_orig='nombre_schema_orig', table_orig='nombre_tabla_orig')
```
Crea la tabla nombre_schema.nombre_tabla con la misma estructura que nombre_schema_orig.nombre_tabla_orig. 

---
### Insertar un dataframe en una tabla
```python
    def insert(self, df: DataFrame, schema: str, table: str, pk: str,
               use_odbc: bool = True, odbc_limit: int = 10000):
        """
        Inserta un DataFrame en una tabla
            :param df: DataFrame a insertar
            :param schema: Schema de la tabla
            :param table: Nombre de la tabla
            :param pk: Primary key de la tabla
            :param use_odbc: Usar ODBC para la inserción
            :param odbc_limit: Límite de filas para usar ODBC
        """
```
**Ejemplo:**
```python
td.insert(df=df, schema='nombre_schema', table='nombre_tabla', pk='nombre_pk')
``` 
Inserta el dataframe df en la tabla nombre_schema.nombre_tabla.  
Si los registros existen, se produce una excepción teradatasql.IntegrityError.  
Al insertar menos de 10000 registros, se utiliza el driver ODBC, caso contrario se utiliza fastload.  
El límite de 10000 registros se puede modificar con el parámetro odbc_limit, y si se especifica use_odbc=False, se fuerza el uso de fastload. 

---
### Actualizar un dataframe en una tabla forzado (insert overwrite/upsert)
```python
    def upsert(self, df: DataFrame, schema: str, table: str, pk: str,
               use_odbc: bool = True, odbc_limit: int = 10000, parser_limit: int = 10000):
        """
        Realiza un upsert en una tabla (insert overwrite)
            :param df: DataFrame a insertar
            :param schema: Schema de la tabla
            :param table: Nombre de la tabla
            :param pk: Primary key de la tabla
            :param use_odbc: Usar ODBC para la inserción
            :param odbc_limit: Límite de filas para usar ODBC
            :param parser_limit: Límite de filas para el parser
        """
```
Hace lo mismo que la función insert, pero si los registros existen, los actualiza.  

**Ejemplo:**
```python
td.upsert(df=df, schema='nombre_schema', table='nombre_tabla', pk='nombre_pk')
```
La actualización se realiza borrando los registros existentes y volviendo a insertarlos.  
El parámetro parser_limit se utiliza para dividir la cantidad de pks en grupos de tamaño parser_limit, y así evitar errores de parser al ejecutar el delete.  
Por lo general no es necesario modificar el parámetro parser_limit, pero si existen excepciones de parser, se puede probar con un valor mas bajo.

---
### Fastload de un dataframe a una tabla
```python
    def fastload(self, df: DataFrame, schema: str, table: str, pk: str, index=False):
        """
        Realiza un fastload en una tabla
            :param df: DataFrame a insertar
            :param schema: Schema de la tabla
            :param table: Nombre de la tabla
            :param pk: Primary key de la tabla
            :param index: Si se debe incluir el índice
        """
```
**Ejemplo:**
```python
td.fastload(df=df, schema='nombre_schema', table='nombre_tabla', pk='nombre_pk')
```
Inserta el dataframe df en la tabla nombre_schema.nombre_tabla utilizando fastload.  
Si los registros existen, se produce una excepción teradatasql.IntegrityError.

---
### Fastload con reintentos
```python
    def retry_fastload(self, df: DataFrame, schema: str, table: str, pk: str, retries: int = 30, retry_sleep: int = 20):
        """
        Realiza un fastload en una tabla con reintentos
            :param df: DataFrame a insertar
            :param schema: Schema de la tabla
            :param table: Nombre de la tabla
            :param pk: Primary key de la tabla
            :param retries: Cantidad de reintentos
            :param retry_sleep: Tiempo de espera entre reintentos
        """
```
Si se produce el error 2663 de fastload (Hay muchas instancias de fastload corriendo), se realiza un retry de la carga luego de esperar retry_sleep segundos.
Esta función es equivalente a insert con el parámetro use_odbc=False.
Por defecto se realizan 30 reintentos con un tiempo de espera de 20 segundos entre reintentos.

**Ejemplo:**
```python
td.retry_fastload(df=df, schema='nombre_schema', table='nombre_tabla', pk='nombre_pk')
```

---
### Obtener la diferencia entre dos tablas
```python
    def diff(self, schema_src: str, table_src: str, schema_dst: str, table_dst: str) -> DataFrame:
        """
        Devuelve un DataFrame con las diferencias entre dos tablas
            :param schema_src: Schema de la tabla origen
            :param table_src: Nombre de la tabla origen
            :param schema_dst: Schema de la tabla destino
            :param table_dst: Nombre de la tabla destino
        """
```
**Ejemplo:**
```python
df_diff = td.diff(schema_src='nombre_schema_src', table_src='nombre_tabla_src', schema_dst='nombre_schema_dst', table_dst='nombre_tabla_dst')
```  
Ejecuta SELECT * FROM nombre_schema_src.nombre_tabla_src MINUS SELECT * FROM nombre_schema_dst.nombre_tabla_dst; y devuelve el resultado en un dataframe.  
Esta función se utiliza para la carga incremental de tablas que no tienen un primary key.  
Es recomendable de todas formas, que las tablas que vayan a realizar cargas incrementales tengan un primary key.  

---
### Carga incremental de un dataframe a una tabla
```python
    def staging_insert(self, df: DataFrame, schema_stg: str, table_stg: str, schema_dst: str, table_dst: str, pk: str):
        """
        Realiza una carga incremental en una tabla
            :param df: DataFrame a insertar
            :param schema_stg: Schema de la tabla staging
            :param table_stg: Nombre de la tabla staging
            :param schema_dst: Schema de la tabla destino
            :param table_dst: Nombre de la tabla destino
            :param pk: Primary key de la tabla
        """
```
Para la carga incremental, primero se sube el lote a cargar a una tabla staging, y luego se realiza un insert desde ese staging a la tabla destino de todos los registros que no existen en la tabla destino.

**Ejemplo:**
```python
td.staging_insert(df=df, schema_stg='nombre_schema_stg', table_stg='nombre_tabla_stg', schema_dst='nombre_schema_dst', table_dst='nombre_tabla_dst', pk='nombre_pk')
```

---
### Upsert incremental de un dataframe a una tabla
```python
    def staging_upsert(self, df: DataFrame, schema_stg: str, table_stg: str, schema_dst: str,
                       table_dst: str, pk: str, parser_limit: int = 10000):
        """
            Realiza un upsert (insert overwrite) incremental en una tabla
            :param df: DataFrame a insertar
            :param schema_stg: Schema de la tabla staging
            :param table_stg: Nombre de la tabla staging
            :param schema_dst: Schema de la tabla destino
            :param table_dst: Nombre de la tabla destino
            :param pk: Primary key de la tabla
            :param parser_limit: Límite de filas para el parser
        """
```
Hace lo mismo que la función staging_insert, pero si los registros existen, los actualiza.

**Ejemplo:**
```python
td.staging_upsert(df=df, schema_stg='nombre_schema_stg', table_stg='nombre_tabla_stg', schema_dst='nombre_schema_dst', table_dst='nombre_tabla_dst', pk='nombre_pk')
```
La actualización se realiza borrando los registros existentes y volviendo a insertarlos al igual que en la función upsert.  
El parámetro parser_limit se utiliza de la misma forma que en la función upsert.
