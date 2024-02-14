## Teradata ODBC (Conexión Cruda ODBC)

Para simplificar un poco las conexiones a Teradata Database se agregó esta nueva funcionalidad.

[Volver al readme principal](../README.md)

La misma consta de solo 3 parámetros:

*	**Host:** *(Requerido, Tipo String)* Indica el servidor de base de datos al cual nos deseamos conectar.
*	**User:** *(Requerido, Tipo String)* Usuario necesario para la conexión al servidor de base de datos.
*	**Password:** *(Requerido, Tipo String)* Contraseña con la que se autentica el usuario para poderse conectar a la base de datos.
*	**Logmech:** *(Opcional, Tipo String)* Indica el mecanismo de autenticación del usuario. Esta función utiliza LDAP por defecto.


Un ejemplo de su uso puede ser el siguiente:

```python
import libgal

con=libgal.teradata(host='servidor', user='tu_user', password='tu_password', logmech='TD2')
```


### TeradataError

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

[Ir al inicio](#teradata-simple-sin-teradataml)