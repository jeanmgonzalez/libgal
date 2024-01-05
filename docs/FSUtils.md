## Utilidades para el manejo de archivos y directorios.
Permite la creación de directorios, eliminar archivos antiguos, cambiar permisos e iniciar el entorno del proyecto.

**Nota:** todas las operaciones se realizan en el directorio de trabajo actual.
Para cambiar el directorio de trabajo actual al raíz del proyecto se debe invocar 
```python
import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
```
Al principio del script principal (ejecutar.py o main.py)

# FSUtils
```python
from libgal.modules.FSUtils import delete_older_files, create_dirs, change_to_public_permissions, create_output_dirs, delete_files, init_env
```

## Funciones

### delete_older_files
```python
def delete_older_files(path: str, max_days: int = 30, dry_run: bool = False, odate = None):
    """
    Elimina los archivos de un directorio que superen la antigüedad máxima
    :param path: Directorio a limpiar
    :param max_days: Antigüedad máxima en días
    :param dry_run: Si es True, no elimina los archivos, solo muestra los que eliminaría
    :param odate: Fecha de referencia para calcular la antigüedad de los archivos
    """
```

Al invocarse la función, se eliminan los archivos que superen la antigüedad máxima en días especificada en el parámetro max_days en la ruta path indicada.
Si se especifica el parámetro dry_run, no se eliminan los archivos, solo se muestra la lista de archivos que se eliminarían.

**Ejemplo:**
```python
delete_older_files('logs', max_days=30)
```
Elimina del directorio logs los archivos que superen los 30 días de antigüedad.
Por defecto la fecha se calcula con la fecha actual, pero se puede especificar una fecha de referencia con el parámetro odate.

**Ejemplo:** 
Eliminar archivos que tengan 30 días de antigüedad a partir del 1 de enero de 2023.
```python
from datetime import datetime

delete_older_files('logs', max_days=30, odate=datetime(2023, 1, 1))
```

Se puede también hacer una ejecución dry_run para ver los archivos que se eliminarían sin eliminarlos realmente.
```python
delete_older_files('logs', max_days=30, dry_run=True)
```

### create_dirs
```python
def create_dirs(dir_list):
    """
    Crea los directorios de la lista (si no existen)
        :param dir_list: Lista de directorios a crear
    """
```
Al invocarse la función se crean los directorios indicados en la lista dir_list (si no existen).
Si el directorio ya existe, no se hace nada.
Si se especifica una ruta completa, se crea el directorio completo con todos los directorios intermedios.

**Ejemplo:**
```python
create_dirs(['logs', 'output', 'input/data'])
```
Crea los directorios logs, output, input e input/data 

### change_to_public_permissions
```python
def change_to_public_permissions(path):
    """
    Cambia los permisos de los archivos de un directorio a 664
        :param path: Directorio a cambiar permisos
    """
```
Al invocarse la función se cambian los permisos de los archivos del directorio indicado en el parámetro path a 664. 
Si ocurre alguna excepción, se muestra un mensaje de error y se continúa con la ejecución. 
Estos permisos habilitan la lectura y escritura para el usuario y el grupo, y solo lectura para otros.

**Ejemplo:**
```python
change_to_public_permissions('logs')
```
Cambia los permisos de los archivos del directorio logs a 664.

### create_output_dirs
```python
def create_output_dirs(home):
    """
    Crea los directorios de salida
        :param home: Directorio raíz
    """
```
Opcionalmente se dispone de una función para crear los directorios de salida 
(output, logs) en el directorio raíz del proyecto.

**Ejemplo:**
```python
create_output_dirs(os.path.dirname(os.path.realpath(__file__)))
```
Ejecutado en el script principal (ejecutar.py o main.py) crea los directorios de salida en el directorio raíz del proyecto.

### delete_files
```python
def delete_files(path, dry_run=False):
    """
    Elimina los archivos de un directorio (si existen)
        :param path: Directorio a limpiar
        :param dry_run: Si es True, no elimina los archivos, solo muestra los que eliminaría
    """
```
Al invocarse la función se eliminan los archivos del directorio indicado en el parámetro path.

**Ejemplo:**
```python
delete_files('output')
```
Elimina todos los archivos del directorio output. 
Al igual que en la función delete_older_files, si se especifica el parámetro dry_run, no se eliminan los archivos, solo se muestra la lista de archivos que se eliminarían.

### init_env
```python
def init_env(home):
    """
    Inicializa el entorno de ejecución
        :param home: Directorio raíz
    """
```

Adicionalmente se dispone de una función para inicializar el entorno de ejecución. 
Esta función crea los directorios de salida (output, logs) en el directorio raíz del proyecto,
cambia los permisos de los archivos del directorio logs a 664 y elimina los archivos del directorio output que superen los 90 días de antigüedad. 

**Ejemplo:**
```python
init_env(os.path.dirname(os.path.realpath(__file__)))
```
