## Request

Función para hacer una llamada web o consumir un servicio de API

Esta función consta de los siguientes parámetros:
*	**url:** *(Requerido, Tipo String)* Indica la url del sitio web o servicio al que deseamos accder.
*	**intentos:** *(Opcional, Tipo Integer)* Indica la cantidad de intentos que debe hacer la función en caso de que falle la llamada. Si no se especifica solo hará una sola llamada.
*	**scraping:** *(Opcional, Tipo Boolean)* Indica si la respuesta de la llamada será usada para scrapear un página web. En caso de ser así solo hay que asignarle el valor True y esta devolverá como respuesta un objeto de BeautifulSoup que nos ayudará a escudriñar el código HTML de la página. Por ser opcional en caso de no especificarse devolvera la respuestá que arroje la página.

[Volver al readme principal](../README.md)

```python
import libgal

pagina=libgal.request(url='pagina.com.ar', intentos=3, scraping=True)

```

[Volver al inicio](#Request)
