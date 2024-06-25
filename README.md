# LibGal

Librería para agilizar el desarrollo de productos de datos en Python.

[Home](https://github.com/jeanmgonzalez/libgal)

## Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Instalación](#instalalación)
- [Proyecto básico](#proyecto-básico)
- [Funcionalidades](#tabla-de-contenidos)
  - [Variables de Entorno](docs/LoadEnv.md)
  - [Registro de Logs](docs/Logger.md)
  - [Módulo TeradataML](docs/Teradata.md)
  - [Ejemplos de TeradataML](docs/by_example/TeradataExamples.md)
  - [Teradata ODBC](docs/SimpleTeradata.md)
  - [SQLAlchemy](docs/SQLAlchemy.md)
  - [Selenium Web Browser Firefox](docs/Selenium.md)
  - [Request](docs/Request.md)
  - [Utilidades del sistema de archivos](docs/FSUtils.md)
  - [Funciones auxiliares](docs/Utils.md)
  - [Contacto](#contacto)


## Descripción General 
Esta librería de Python está desarrollada con la finalidad de proveer de un entorno de trabajo más amigable y eficiente para el desarrollo de productos de datos.   
Entre las funcionalidades que provee esta librería se encuentran diversas abstracciones que permiten la conexión a bases de datos, realizar consultas SQL, cargar y descargar dataframes de Pandas, como también, 
la invocación de un Web Browser de Selenium, la manipulación de archivos de texto y la creación de registros logs, entre otras.

Su público objetivo son los desarrolladores de productos de datos que deseen agilizar el desarrollo de sus aplicaciones y automatizaciones.

## Instalación

La instalación de esta librería se hace mediante siguiente sentencia:

```python
pip install libgal
```

Para más información sobre la instalación de la librería, por favor consulte la [documentación de instalación](docs/Installation.md).

[Ir arriba](#libgal)

## Proyecto Básico

A continuación, se muestra un ejemplo de como inicializar un nuevo proyecto con libgal:

```python
import logging
import os
from libgal.modules.Logger import Logger
from libgal.modules.FSUtils import init_env

# inicializo los directorios de salida principales en el directorio raíz del proyecto
init_env(os.path.dirname(os.path.abspath(__file__)))

# invoco el logger de la librería y le especifico el directorio de salida de los logs (ruta relativa al directorio raíz del proyecto)
logger_wrapper = Logger()
logger_wrapper.set_outputdir(dirname='./logs', log_format='json')
logger = logger_wrapper.get_logger()

if __name__ == "__main__":
    # establezco el nivel de log a DEBUG
    logger.setLevel(logging.DEBUG)
    logger.info("Inicio de la aplicación")
    # resto del código ...
```

**Salida:**
```text
2024-02-14 15:31:16,902 PID: 2828 (1824666079424) MainThread [INFO | Logger.py:76] > Generate new instance, hash = 1824666079424 
2024-02-14 15:31:16,902 PID: 2828 (1824666079424) MainThread [INFO | FSUtils.py:46] > Creando directorio output 
2024-02-14 15:31:16,903 PID: 2828 (1824666079424) MainThread [INFO | FSUtils.py:46] > Creando directorio logs 
2024-02-14 15:31:16,903 PID: 2828 (1824666079424) MainThread [INFO | FSUtils.py:46] > Creando directorio db 
2024-02-14 15:31:16,903 PID: 2828 (1824666079424) MainThread [INFO | FSUtils.py:76] > Cambiando permisos de output 
2024-02-14 15:31:16,904 PID: 2828 (1824666079424) MainThread [INFO | FSUtils.py:76] > Cambiando permisos de logs 
2024-02-14 15:31:16,904 PID: 2828 (1824666079424) MainThread [INFO | FSUtils.py:60] > Cambiando permisos de logs\log_2024-02-14.log a 0o664 
2024-02-14 15:31:16,904 PID: 2828 (1824666079424) MainThread [INFO | FSUtils.py:76] > Cambiando permisos de db 
2024-02-14 15:31:16,906 PID: 2828 (1824666079424) MainThread [INFO | FSUtils.py:60] > Cambiando permisos de logs\log_2024-02-14.log a 0o664 
{'time':'02/14/2024 03:31:16 PM', 'pid': '2828', 'instance_hash': '1824666079424', 'thread', 'MainThread', 'name': 'libgal.modules.Logger', 'level': 'INFO', 'file': 'scratch_3.py', 'lineno': 17, 'message': 'Inicio de la aplicación'}
```

## Contacto

Jean González - [@jeanmgonzalez](https://github.com/jeanmgonzalez)

[![LinkedIn][linkedin-shield]][linkedin-url-jean]

Julian Girandez - [@julgiraldez](https://github.com/JuLGiraldez)

[![LinkedIn][linkedin-shield]][linkedin-url-juli]

Sebastian Wilwerth - [@swilwerth](https://github.com/VideoMem)

[![LinkedIn][linkedin-shield]][linkedin-url-seba]

[Volver al inicio](#libgal)

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