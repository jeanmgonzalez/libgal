## HTML_Parser

Está función sirve para hacer búsquedas rápidas de etiquetas y textos dentro de un código HTML mediante funciones nativas de Beautiful Soup. Para esto solo será necesario instanciar la función en una variable pasándole por parámetro un string o variable de tipo string contentiva del código HTML a trabajar, tal cómo se muestra a continuación:

[Volver al readme principal](../README.md)

```python
import libgal

html='<html><head></head><body>Sacré bleu!</body></html>'

soup=libgal.html_parser(html)
```

[Volver al inicio](#html_parser)
