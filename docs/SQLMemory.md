# SQLMemory

## Interfaz simplificada para la carga de Dataframes a SQL en memoria

### Descripción
La clase SQLMemory permite cargar Dataframes a SQL en memoria y hacer consultas sobre ellos.

[Volver al readme principal](../README.md)

### Importar la librería
```python
from libgal.modules.SQLMemory import SQLMemory
```

### Crear una instancia
```python
    sql = SQLMemory('test.db')
```

## ejemplo
```python
    sql = SQLMemory('test.db')
```

## Volcar el contenido de la memoria a un archivo
```python
    sql.vacuum()
```

## Ejecutar una sentencia SQL que no retorna resultados

# ejemplo
```python
    sql.do('CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)')
```
