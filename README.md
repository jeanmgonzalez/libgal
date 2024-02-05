
<a name="readme-top"></a>

<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->

<!-- PROJECT LOGO -->

<div align="center">

  <h1 align="center">LibGal</h1>

  <p align="center">
    Librería para agilizar el desarrollo en Python.
    <br/>
    <br/>
    <a href="https://github.com/jeanmgonzalez/libgal"><strong>Explorar el proyecto»</strong></a>
    <br />
    <br />
    <a href="https://github.com/jeanmgonzalez/libgal/issues">Reportar error</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Tabla de Contenidos</summary>
  <ol>
    <li>
      <a href="#descripción-general">Descripción General</a>
    </li>
    <li>
      <a href="#instalalación">Instalación</a>
    </li>
    <li>
      <a href="#funcionalidades">Funcionalidades</a>
      <ul>
        <li><a href="#variables-de-entorno">Variables de Entorno</a></li>
        <li><a href="#registro-de-logs">Registro de Logs</a></li>
        <li><a href="#selenium-web-browser-firefox">Selenium Web Browser Firefox</a></li>
        <li><a href="#teradata">Teradata</a>
          <ul>
            <li><a href="docs/Teradata.md">Fastload y cargas incrementales</a></li>
          </ul>
          <ul>
            <li><a href="#teradataerror">TeradataError</a></li>
          </ul>
        </li>
        <li>
          <a href="#sqlalchemy">SQLAlchemy</a>
          <ul>
            <li><a href="#select">Select</a></li>
            <li><a href="#insert">Insert</a></li>
            <li><a href="#query">Query</a></li>
            <li><a href="#sqlalchemyerror">SQLAlchemyError</a></li>
          </ul>
        </li>
        <li><a href="docs/FSUtils.md">Utilidades del filesystem</a></li>
      </ul>
    </li>
    <li><a href="#contacto">Contacto</a></li>
  </ol>
</details>

<br/>

<!-- ABOUT THE LIBRARY -->

## **Descripción General** 
<br>
Esta librería python fue desarrollada con la finalidad de agilizar el desarrollo de aplicaciones con funciones configurables minimizando de esta forma nuestro código python, evitando a su vez la replicación de  código en distintos proyectos, permitiendonos centrar en la funcionalidad principal de la aplicación a desarrollar.

<p align="right">(<a href="#readme-top">Ir arriba</a>)</p>

<!-- INSTALACIÓN -->
## **Instalación**
<br>

La instación de esta librería se hace mediante siguiente sentencia:

```python
pip install libgal
```

Para mas información sobre la instalación de la librería, por favor consulte la [documentación de instalación](docs/Installation.md).

<p align="right">(<a href="#readme-top">Ir arriba</a>)</p>

<!-- FUNCIONALIDADES -->
## **Funcionalidades**
<br>
Para hacer uso de las diferentes funcionalidades de esta librería basta con importar la misma en nuestro código con la siguiente sentencia:

<br>

```python
import libgal
```

<br>

Una vez importada la librería solo nos queda instanciar en una variable la función que necesitemos tal como se muestra a continuación.

<br>

```python
browser=libgal.variables_entorno()
```

<!-- FUNCIONALIDAD - Variables de Entorno -->
### **Variables de Entorno**

<br>

Para poder usar las variables de entorno de forma local con esta librería será necesario crear un archivo de texto cuyo nombre y extensión será “.env”. Dentro de este mismo archivo “.env” podemos especificar todas las variables secrets y configmap que utilizará nuestra aplicación, tal como se muestra en el siguiente ejemplo:

<br>

```sh
#SECRETS
USERNAME = usuario@correo.com
PASSWORD = contraseña

#CONFIGMAP
API_PREDICT=https://url.com/predict
API_AUDIENCIAS=https://url.com/audiencias
CANT_POST=10 #Cantidad de últimos posts a descargar
```
<br>

Es importante mencionar que al momento de desplegar nuestra aplicación no se debe subir este archivo “.env” ya que solo es para ejecuciones y pruebas en modo local simulando estar en el entorno productivo donde se debe manejar un sistema de secrets.

<br>

Ahora bien, para poder usar estas variables dentro de nuestro código solo será necesario importar la librería LIBGAL e instanciar en una variable la función VARIABLES_ENTORNO, indicando como parametro la ruta y nombre del archivo .env y así poder acceder a las variables de entorno indicado en el mismo, tal cómo se muestra en el siguiente ejemplo:

<br>

```python
import libgal

ve=libgal.variables_entorno('.env')

api_predict=ve['API_PREDICT']
api_audiencias=ve['API_AUDIENCIAS']
```

<br>

Nótese que para invocar los nombres de las variables es necesario escribirlas en mayúscula.

<p align="right">(<a href="#readme-top">Ir arriba</a>)</p>

<!-- Registro de Logs -->
### **Registro de Logs**  

<br>

Haciendo uso de esta librería no nos tenemos que preocupar por la configuración de nuestros registros logs, ya que la misma se encarga de ello mediante unos pocos pasos. Para hacer esto, solo debemos llamar la función LOGGER de la librería y asignarla a una variable para poder usar en el resto de nuestro código.

<br>

La función LOGGER consta de dos parámetros de configuración de tipo string:

<br>

*	**format_output:** *(Requerido, Tipo String)* Indica el tipo de formato para el registro log de nuestra aplicación. Por los momentos consta de dos tipos: “JSON” usado para los logs dentro del entorno Openshift y “CSV” para generar el log en una sola línea separados por coma (,).

<br>

*	**app_name:** *(Requerido, Tipo String)* En este parámetro especificaremos el nombre de nuestra aplicación. Recordemos que nuestro archivo Python principal deberá llamar APP.PY.

<br>

Para crear un registro log mediante esta función en nuestra aplicación solo debemos hacer uso de nuestra variable tipo LOGGER de forma muy similar al “print” de Python pero con un agregado adicional y es que podemos definir el nivel de Log para cada registro, tal como lo veremos en el siguiente código de ejemplo:

<br>

```python
Import libgal

log=libgal.logger(format_output="JSON", app_name="Instagram")

log.info("Esto es un registro informativo")
log.error("Esto es un registro de error")
log.warning("Esto es un registro de advertencia")
log.critical("Esto es un registro de error crítico")
log.exception("Esto es un registro de excepción")
log.log("Esto es un registro de log")
```
Ver tests en [FileLoggerTests](tests/FileLoggerTests.py) para mas info sobre logger con salida en archivo. 

<p align="right">(<a href="#readme-top">ir arriba</a>)</p>



<!-- Selenium Web Browser Firefox -->
### **Selenium Web Browser Firefox**

<br>

Mediante la librería podemos hacer la invocación de un Web Browser de Selenium para nuestras automatizaciones, test y/o extracciones de datos de cualquier página web. Esto se logra invocando la función Firefox de la librería e instanciándola a una variable. 

<br>

La función consta de 4 parámetros de configuración:

<br>

*	**webdriver_path:** *(Requerido, Tipo String)*  Ruta del driver geckodriver utilizado para levantar e invocar el Web Browser de Firefox.

<br>

*	**browser_path:** *(Requerido, Tipo String)* Ruta del ejecutable Firefox.exe del servidor o equipo local necesario para levantar el Web Browser.

<br>

*	**url:** *(Requerido, Tipo String)* Dirección Web con la que vamos a mediante el Web Browser.

<br>

* Hidden: (Opcional, Tipo Booleano) Indica si el Web Browser se oculta durante su ejecución. False predeterminado.

<br>

Ejemplo:

```python
import libgal

browser=libgal.firefox(webdriver_path=r"C:\webdrivers\geckodriver.exe",browser_path=r"C:\Program Files\Mozilla Firefox\firefox.exe",url="https://bolsar.info/Cauciones.php")
```
<p align="right">(<a href="#readme-top">ir arriba</a>)</p>

### **Teradata**

<br>

Para simplificar un poco las conexiones a Teradata Database se agregó esta nueva funcionalidad.

La misma consta de solo 3 parámetros:

*	**Host:** *(Requerido, Tipo String)* Indica el servidor de base de datos al cual nos deseamos conectar.

*	**User:** *(Requerido, Tipo String)* Usuario necesario para la conexión al servidor de base de datos.

*	**Password:** *(Requerido, Tipo String)* Contraseña con la que se autentica el usuario para poderse conectar a la base de datos.

*	**Logmech:** *(Opcional, Tipo String)* Indica el mecanismo de autenticación del usuario. Esta función utiliza LDAP por defecto.

<br>

Un ejemplo de su uso puede ser el siguiente:

```python
import libgal

con=libgal.teradata(host='servidor', user='tu_user', password='tu_password', logmech='TD2')
```

<br>

### TeradataError

<br>

Mediante esta función podemos acceder a las diferentes excepciones de error de TeradataSQL, tal como se muestra en el siguiente ejemplo:

```python
import libgal

conexion=libgal.teradata(host='host', user='user', password='password', logmech='TD2')

try:
  data=('1', 'Descripción 1')
  query="INSERT INTO esquema.tabla(codigo, descripcion) VALUES (?,?)"

  with conexion.cursor() as cursor:
      cursor.execute(query,data)
      conexion.commit()
  
  print("Los datos fueron almacenados correctmente.")

except libgal.TeradataError as e:

  print(e)
  
```

Ver tests en [Teradata_Basic_Tests](tests/Teradata_Basic_Test.py) para mas info.

<p align="right">(<a href="#readme-top">Ir arriba</a>)</p>

### **HTML_Parser**

<br>

Está función sirve para hacer búsquedas rápidas de etiquetas y textos dentro de un código HTML mediante funciones nativas de Beautiful Soup. Para esto solo será necesario instanciar la función en una variable pasándole por parámetro un string o variable de tipo string contentiva del código HTML a trabajar, tal cómo se muestra a continuación:

<br>

```python
import libgal

html='<html><head></head><body>Sacré bleu!</body></html>'

soup=libgal.html_parser(html)
```

<p align="right">(<a href="#readme-top">Ir arriba</a>)</p>

### **SQLAlchemy**

<br>

Esta función encapsula algunas de las funcionalidades de SQLAlchemy. La funcionalidad principal de su uso consiste en la adopción de la progamación de acceso a datos mediante ORMs lo que nos permite cambiar de base de datos sin necesidad de cambiar las instrucciones CRUD dentro de nuestro código.

<br>

Para la instanciación de la misma es necesario definir los siguientes parámetros:

*	**Host:** *(Requerido, Tipo String)* Indica el servidor de base de datos al cual nos deseamos conectar.

*	**User:** *(Requerido, Tipo String)* Usuario necesario para la conexión al servidor de base de datos.

*	**Password:** *(Requerido, Tipo String)* Contraseña con la que se autentica el usuario para poderse conectar a la base de datos.

*	**Driver:** *(Requerido, Tipo String)* Indica el tipo de base de datos al que nos estamos conectando. Por los momentos solo podemos definir los siguientes valores: Teradata y/o MySQL.

*	**Logmech:** *(Opcional, Tipo String)* Indica el mecanismo de autenticación del usuario. Es función utiliza LDAP por defecto.

Para comenzar a interactuar con esta función podemos seguir el siguiente ejemplo:

<br>

```python
import libgal

con=libgal.sqlalchemy(host='host', user='usuario', password='password', driver='teradata', logmech='TD2')
```

<br>

Para poder intectuar con las tablas de la base es necesario crear objetos que harán referencia a las mismas tal como se muestra en el siguiente ejemplo:

<br>

```python
#Creación de modelos de Tablas
Base = con.Base()

# Defino clase de la tabla
class clase_tabla(Base):
    __tablename__ = 'nombre_tabla'
    __table_args__ = {'schema': 'nombre_esquema'}
    campo_clave = libgal.Column(libgal.Integer, primary_key=True)
    descripcion = libgal.Column(libgal.String)

    def __repr__(self):
        return f"<dato(campo_clave='{self.campo_clave}', descripcion={self.descripcion})>"
```
<br>

Notese que se creó una clase con los mismos atributos definidos para la creación de la tabla en el motor de la base de datos. A partir de aquí podemos crear objetos con los que podemos interactuar y que impactarán en la tabla de la base.

Se recomienda usar sesiones para interactuar con las tablas. Estas son definidas de la siguiente forma:

<br>

```python
session=con.Session()
```

<br>

### Select

<br>

Para listar todos los registros y campos de una tabla con SQLAlchemy solo debemos crear un objeto de la siguiente forma:

<br>

```python
query_tabla = session.query(clase_tabla)
datos=query_tabla.all()
```
<br>

En caso de que se quiera hacer un select con campos específicos y que además se quiera filtrar los registros con algunos valores de uno o más campos, lo podemos hacer de la siguiente manera:

<br>

```python
query_tabla = session.query(clase_tabla.campo_clave, clase_tabla.descripcion).filter(clase_tabla.campo_clave==1)
datos=query_tabla.all()
```

<br>

### Insert

<br>

Para agregar un registro en la tabla solo creamos un objeto mediante la clase de la tabla que vamos a trabajar, tal como se muestra en el siguiente ejemplo:

<br>

```python
nuevo_dato = clase_tabla(campo_clave=1, descripcion='Descripción del registro')

session = bd.Session()
session.add(nuevo_dato)
session.commit()
session.close()
```
<br>

### Query

<br>

En caso de que se requiera hacer una query especializada, con campos calculados o que implique dos o más tablas, mediante la conexión que creamos para interactuar con la base de datos, disponibilizamos un método llamado QUERY, el cual puede ser invocado como se muestra a continuación:

<br>

```python
import libgal

con=libgal.sqlalchemy(host='host', user='usuario', password='password', driver='teradata')

otra_query=con.query("select * from tabla where campo='valor'")
```
<br>

### InsertDataframe

Esta función permite insertar los datos de un Dataframe de Pandas en una tabla siempre y cuando tenga los mismos nombres de campo que el Dataframe.

```python
import libgal
import pandas

d = {'col1': [1, 2], 'col2': [3, 4]}

dataframe = pandas.DataFrame(data=d)

con=libgal.sqlalchemy(host='host', user='usuario', password='password', driver='teradata')

conn.InsertDataframe(pandas_dataframe=dataframe,database='esquema', table='tabla')
```
Ver tests en [SQLAlchemyTests](tests/SQLAlchemy_test.py) para mas info.  
Si el dataframe tiene más de 10000 filas es recomendable utilizar [Fastload](docs/Teradata.md#fastloaddf-dataframe-schema-str-table-str-pk-str-index-bool--false) para la carga de datos.   
Para mas detalles ver: [Teradata](docs/Teradata.md) 
<br>


### SQLAlchemyError

<br>

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
Ver tests en [SQLAlchemyTests](tests/SQLAlchemy_test.py) para mas info. 
<p align="right">(<a href="#readme-top">Ir arriba</a>)</p>


<!-- CONTACTO-->
## Contacto

<br>

Jean González - [@jeanmgonzalez](https://github.com/jeanmgonzalez)

[![LinkedIn][linkedin-shield]][linkedin-url-jean]

<br>

Julian Girandez - [@julgiraldez](https://github.com/JuLGiraldez)

[![LinkedIn][linkedin-shield]][linkedin-url-juli]

<br>

Sebastian Wilwerth - [@swilwerth](https://github.com/VideoMem)

[![LinkedIn][linkedin-shield]][linkedin-url-seba]

<br>


Link del proyecto: [https://github.com/jeanmgonzalez/libgal](https://github.com/jeanmgonzalez/libgal)

<br>


<p align="right">(<a href="#readme-top">ir arriba</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->

[contributors-shield]: https://img.shields.io/github/contributors/othneildrew/Best-README-Template.svg?style=for-the-badge
[contributors-url]: https://github.com/othneildrew/Best-README-Template/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/othneildrew/Best-README-Template.svg?style=for-the-badge
[forks-url]: https://github.com/othneildrew/Best-README-Template/network/members
[stars-shield]: https://img.shields.io/github/stars/othneildrew/Best-README-Template.svg?style=for-the-badge
[stars-url]: https://github.com/othneildrew/Best-README-Template/stargazers
[issues-shield]: https://img.shields.io/github/issues/othneildrew/Best-README-Template.svg?style=for-the-badge
[issues-url]: https://github.com/Banco-Galicia/libgal/issues
[license-shield]: https://img.shields.io/github/license/othneildrew/Best-README-Template.svg?style=for-the-badge
[license-url]: https://github.com/othneildrew/Best-README-Template/blob/master/LICENSE.txt
[linkedin-shield]:https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url-jean]: https://www.linkedin.com/in/bidata/
[linkedin-url-juli]: https://www.linkedin.com/in/julian-leandro-giraldez/
[linkedin-url-seba]: https://ar.linkedin.com/in/sebastian-wilwerth-66781922b?trk=public_profile_browsemap
[product-screenshot]: images/screenshot.png
[Next.js]: https://img.shields.io/badge/next.js-000000?style=for-the-badge&logo=nextdotjs&logoColor=white
[Next-url]: https://nextjs.org/
[React.js]: https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB
[React-url]: https://reactjs.org/
[Vue.js]: https://img.shields.io/badge/Vue.js-35495E?style=for-the-badge&logo=vuedotjs&logoColor=4FC08D
[Vue-url]: https://vuejs.org/
[Angular.io]: https://img.shields.io/badge/Angular-DD0031?style=for-the-badge&logo=angular&logoColor=white
[Angular-url]: https://angular.io/
[Svelte.dev]: https://img.shields.io/badge/Svelte-4A4A55?style=for-the-badge&logo=svelte&logoColor=FF3E00
[Svelte-url]: https://svelte.dev/
[Laravel.com]: https://img.shields.io/badge/Laravel-FF2D20?style=for-the-badge&logo=laravel&logoColor=white
[Laravel-url]: https://laravel.com
[Bootstrap.com]: https://img.shields.io/badge/Bootstrap-563D7C?style=for-the-badge&logo=bootstrap&logoColor=white
[Bootstrap-url]: https://getbootstrap.com
[JQuery.com]: https://img.shields.io/badge/jQuery-0769AD?style=for-the-badge&logo=jquery&logoColor=white
[JQuery-url]: https://jquery.com 