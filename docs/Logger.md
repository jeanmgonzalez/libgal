## Registro de Logs  

[Volver al readme principal](../README.md)

Haciendo uso de esta librería no nos tenemos que preocupar por la configuración de nuestros registros logs, ya que la misma se encarga de ello mediante unos pocos pasos.   
Para hacer esto, solo debemos llamar la función LOGGER de la librería y asignarla a una variable para poder usarla en el resto de nuestro código.

La función LOGGER consta de dos parámetros de configuración de tipo string:
*	**format_output:** *(Requerido, Tipo String)* Indica el tipo de formato para el registro log de nuestra aplicación. Por los momentos consta de dos tipos: “JSON” usado para los logs dentro del entorno Openshift y “CSV” para generar el log en una sola línea separados por coma (,).
*	**app_name:** *(Requerido, Tipo String)* En este parámetro especificaremos el nombre de nuestra aplicación. Recordemos que nuestro archivo Python principal deberá llamar APP.PY.

Para crear un registro log mediante esta función en nuestra aplicación solo debemos hacer uso de nuestra variable tipo LOGGER de forma muy similar al “print” de Python pero con un agregado adicional y es que podemos definir el nivel de Log para cada registro, tal como lo veremos en el siguiente código de ejemplo:

```python
import libgal

log=libgal.logger(format_output="JSON", app_name="Instagram")

log.info("Esto es un registro informativo")
log.error("Esto es un registro de error")
log.warning("Esto es un registro de advertencia")
log.critical("Esto es un registro de error crítico")
log.exception("Esto es un registro de excepción")
log.log("Esto es un registro de log")
log.debug("Esto es un registro de depuración")
```
Ver tests en [FileLoggerTests](../tests/FileLoggerTests.py) para mas info sobre logger con salida en archivo. 

[Volver al inicio](#registro-de-logs)
