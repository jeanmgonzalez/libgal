## Variables de Entorno

Para poder usar las variables de entorno de forma local con esta librería será necesario crear un archivo de texto cuyo nombre y extensión será “.env”.  
Dentro de este mismo archivo “.env” podemos especificar todas las variables secrets y configmap que utilizará nuestra aplicación, tal como se muestra en el siguiente ejemplo:

[Volver al readme principal](../README.md)

```python
from libgal import variables_entorno
browser=variables_entorno('archivo.env')
```

archivo.env
```sh
#SECRETS
USERNAME = usuario@correo.com
PASSWORD = contraseña

#CONFIGMAP
API_PREDICT=https://url.com/predict
API_AUDIENCIAS=https://url.com/audiencias
CANT_POST=10 # Cantidad de últimos posts a descargar
```

Es importante mencionar que al momento de desplegar nuestra aplicación no se debe subir este archivo “.env” ya que solo es para ejecuciones y pruebas en modo local simulando estar en el entorno productivo donde se debe manejar un sistema de secrets.

Ahora bien, para poder usar estas variables dentro de nuestro código solo será necesario importar la librería LIBGAL e instanciar en una variable la función VARIABLES_ENTORNO, indicando como parametro la ruta y nombre del archivo .env y así poder acceder a las variables de entorno indicado en el mismo, tal cómo se muestra en el siguiente ejemplo:

```python
import libgal

ve=libgal.variables_entorno('.env')

api_predict=ve['API_PREDICT']
api_audiencias=ve['API_AUDIENCIAS']
```

Nótese que para invocar los nombres de las variables es necesario escribirlas en mayúscula.

[Ir arriba](#variables-de-entorno)

