# SQLAlchemy

Libgal provee todas las funcionalidades de SQLAlchemy. 

SQLAlchemy es una biblioteca de Python ampliamente utilizada para trabajar con bases de datos relacionales de una manera flexible y poderosa.   
Proporciona una capa de abstracción sobre los motores de bases de datos, lo que significa que puedes interactuar con diferentes tipos de bases de datos (como Teradata, SQLServer, SQLite, PostgreSQL, MySQL, entre otros) de manera uniforme, utilizando una sintaxis consistente en Python.

[Volver al readme principal](../README.md)

## Índice
- [Introducción](#introducción)
- [Conectar a una base de datos con SQLAlchemy](#conectar-a-una-base-de-datos-con-sqlalchemy)
- [Select](#select)
- [Insert](#insert)
- [Query](#query)
- [InsertDataframe](#insertdataframe)
- [SQLAlchemyError](#sqlalchemyerror)


## Introducción

**Objetivo Principal:**

El principal objetivo de SQLAlchemy es proporcionar a los desarrolladores una forma intuitiva y potente de trabajar con bases de datos relacionales, permitiendo la creación, consulta, modificación y eliminación de datos de manera eficiente y segura.

**Componentes Principales:**

1. **Core:** Este es el núcleo de SQLAlchemy, que proporciona una API de nivel más bajo para interactuar con la base de datos. Incluye la creación y manipulación de tablas, expresiones SQL, y manejo de transacciones.
2. **ORM (Mapeo Objeto-Relacional):** SQLAlchemy también proporciona un ORM completo que permite mapear clases Python a tablas en la base de datos, facilitando así la interacción con los datos a través de objetos Python en lugar de escribir consultas SQL directamente.


## Casos de uso típicos

1. **Desarrollo de Aplicaciones Web:** SQLAlchemy es ampliamente utilizado en el desarrollo de aplicaciones web para interactuar con bases de datos. Permite a los desarrolladores crear modelos de datos utilizando clases Python y luego realizar operaciones CRUD (Crear, Leer, Actualizar, Eliminar) en la base de datos de manera transparente.

2. **Análisis de Datos:** Para aplicaciones que requieren análisis de datos, SQLAlchemy facilita la extracción y manipulación de datos almacenados en bases de datos relacionales. Puedes utilizar expresiones SQL complejas y funciones de agregación para realizar consultas avanzadas.

3. **Migración de Bases de Datos:** SQLAlchemy ofrece herramientas para facilitar la migración de esquemas de bases de datos, lo que permite a los desarrolladores realizar cambios en la estructura de la base de datos de manera controlada.

4. **Automatización de Tareas de Bases de Datos:** Puedes utilizar SQLAlchemy para automatizar tareas repetitivas relacionadas con bases de datos, como la limpieza de datos, la generación de informes periódicos y la administración de usuarios y permisos.

5. **Desarrollo de APIs RESTful:** SQLAlchemy se puede utilizar en conjunto con frameworks web como Flask o Django para crear APIs RESTful que proporcionen acceso a los datos almacenados en la base de datos de una manera segura y eficiente.

6. **Integración con Frameworks de Análisis de Datos:** Para proyectos que requieren tanto el análisis de datos como la persistencia de los mismos, SQLAlchemy puede integrarse fácilmente con frameworks de análisis de datos como Pandas, facilitando la manipulación de datos entre diferentes entornos.

## Ventajas

1. **Abstracción:** SQLAlchemy ofrece una capa de abstracción sobre el lenguaje SQL, lo que te permite trabajar con diferentes tipos de bases de datos sin necesidad de modificar tu código.

2. **Flexibilidad:** Permite mapear objetos Python a tablas de la base de datos mediante el uso de un mapeo relacional de objetos (ORM), lo que te da mucha flexibilidad a la hora de definir tu modelo de datos.

3. **Potente:** Ofrece una API completa para realizar todo tipo de operaciones CRUD (Create, Read, Update, Delete), así como consultas complejas y transacciones.

4. **Extensible:** Cuenta con una amplia comunidad de desarrolladores que han creado plugins y extensiones para ampliar su funcionalidad.

5. **Seguridad:** Te ayuda a prevenir la **inyección SQL**, lo que puede mejorar la seguridad de tu aplicación.

## Desventajas

Si bien SQLAlchemy es una herramienta poderosa y flexible, también tiene algunas desventajas que debes tener en cuenta:

1. **Curva de aprendizaje:** SQLAlchemy es una biblioteca compleja con una API extensa. Esto puede dificultar su aprendizaje y uso, especialmente para principiantes.

2. **Rendimiento:** El uso de un ORM como SQLAlchemy puede tener un impacto en el rendimiento de las consultas, especialmente si se realizan operaciones complejas.

3. **Complejidad:** El uso de un ORM como SQLAlchemy puede añadir una capa de complejidad a tu código, lo que puede dificultar su comprensión y mantenimiento.

4. **Problemas con consultas complejas:** Aunque SQLAlchemy permite realizar consultas complejas, estas pueden ser difíciles de escribir y entender.

5. **Falta de control:** Al usar un ORM, pierdes cierto control sobre las consultas SQL que se generan. Esto puede ser un problema si necesitas optimizar el rendimiento o realizar operaciones específicas que no son compatibles con el ORM.

6. **Depuración:** La depuración de errores puede ser más difícil cuando se usa un ORM, ya que hay que tener en cuenta la capa de abstracción que añade.


## Conectar a una base de datos con SQLAlchemy

Para la instanciación de la misma es necesario definir los siguientes parámetros:

*	**Host:** *(Requerido, Tipo String)* Indica el servidor de base de datos al cual nos deseamos conectar.

*	**User:** *(Requerido, Tipo String)* Usuario necesario para la conexión al servidor de base de datos.

*	**Password:** *(Requerido, Tipo String)* Contraseña con la que se autentica el usuario para poderse conectar a la base de datos.

*	**Driver:** *(Requerido, Tipo String)* Indica el tipo de base de datos al que nos estamos conectando. Por los momentos solo podemos definir los siguientes valores: Teradata y/o MySQL.

*	**Logmech:** *(Opcional, Tipo String)* Indica el mecanismo de autenticación del usuario. Es función utiliza LDAP por defecto.

Para comenzar a interactuar con esta función podemos seguir el siguiente ejemplo:

```python
import libgal

engine = libgal.sqlalchemy(host='host', user='usuario', password='password', driver='teradata', logmech='TD2')
```

Para poder interactuar con las tablas de la base es necesario crear objetos que harán referencia a las mismas tal como se muestra en el siguiente ejemplo:

```python
#Creación de modelos de Tablas
Base = engine.Base()

# Defino clase de la tabla
class clase_tabla(Base):
    __tablename__ = 'nombre_tabla'
    __table_args__ = {'schema': 'nombre_esquema'}
    campo_clave = libgal.Column(libgal.Integer, primary_key=True)
    descripcion = libgal.Column(libgal.String)

    def __repr__(self):
        return f"<dato(campo_clave='{self.campo_clave}', descripcion={self.descripcion})>"
```

Nótese que se creó una clase con los mismos atributos definidos para la creación de la tabla en el motor de la base de datos. A partir de aquí podemos crear objetos con los que podemos interactuar y que impactarán en la tabla de la base.

Se recomienda usar sesiones para interactuar con las tablas. Estas son definidas de la siguiente forma:

```python
session=engine.Session()
```

[Volver al inicio](#sqlalchemy)

### Select

Para listar todos los registros y campos de una tabla con SQLAlchemy solo debemos crear un objeto de la siguiente forma:

```python
query_tabla = session.query(clase_tabla)
datos=query_tabla.all()
```

En caso de que se quiera hacer un select con campos específicos y que además se quiera filtrar los registros con algunos valores de uno o más campos, lo podemos hacer de la siguiente manera:

```python
query_tabla = session.query(clase_tabla.campo_clave, clase_tabla.descripcion).filter(clase_tabla.campo_clave==1)
datos=query_tabla.all()
```

[Volver al inicio](#sqlalchemy)

### Insert

Para agregar un registro en la tabla solo creamos un objeto mediante la clase de la tabla que vamos a trabajar, tal como se muestra en el siguiente ejemplo:

```python
nuevo_dato = clase_tabla(campo_clave=1, descripcion='Descripción del registro')

session = bd.Session()
session.add(nuevo_dato)
session.commit()
session.close()
```

[Volver al inicio](#sqlalchemy)

### Query

En caso de que se requiera hacer una query especializada, con campos calculados o que implique dos o más tablas, mediante la conexión que creamos para interactuar con la base de datos, disponibilizamos un método llamado QUERY, el cual puede ser invocado como se muestra a continuación:

```python
import libgal

engine = libgal.sqlalchemy(host='host', user='usuario', password='password', driver='teradata')

otra_query=engine.query("select * from tabla where campo='valor'")
```

[Volver al inicio](#sqlalchemy)

### InsertDataframe

Esta función permite insertar los datos de un Dataframe de Pandas en una tabla siempre y cuando tenga los mismos nombres de campo que el Dataframe.

```python
import libgal
import pandas

d = {'col1': [1, 2], 'col2': [3, 4]}

dataframe = pandas.DataFrame(data=d)

con = libgal.sqlalchemy(host='host', user='usuario', password='password', driver='teradata')

con.insert(pandas_dataframe=dataframe, database='esquema', table='tabla')
```
Ver tests en [SQLAlchemyTests](../tests/SQLAlchemy_test.py) para más info.  
Si el dataframe tiene más de 10000 filas es recomendable utilizar [Fastload](./Teradata.md#fastloaddf-dataframe-schema-str-table-str-pk-str-index-bool--false) para la carga de datos.   
Para más detalles ver: [Teradata](./Teradata.md)

[Volver al inicio](#sqlalchemy)

### SQLAlchemyError

Mediante esta función podemos acceder a las diferentes excepciones de error de SQLAlchemy, tal como se muestra en el siguiente ejemplo:

```python
import libgal

con=libgal.sqlalchemy(host='host', user='user', password='password', driver='teradata', logmech='TD2')

with con.Session() as session:

    try:
        session.add(nuevo_dato)
        session.commit()   
        print("Datos almacenadas correctamente")
    except libgal.SQLAlchemyError as e:
        session.rollback()
        print(e)
```
Ver tests en [SQLAlchemyTests](../tests/SQLAlchemyTests.py) para mas info. 

[Volver al inicio](#sqlalchemy)