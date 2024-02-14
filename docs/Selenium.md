### Selenium Web Browser Firefox

Mediante la librería podemos hacer la invocación de un Web Browser de Selenium para nuestras automatizaciones, test y/o extracciones de datos de cualquier página web.    
Esto se logra invocando la función Firefox de la librería e instanciándola a una variable. 

[Volver al readme principal](../README.md)

La función consta de 4 parámetros de configuración:

*	**webdriver_path:** *(Requerido, Tipo String)*  Ruta del driver geckodriver utilizado para levantar e invocar el Web Browser de Firefox.
*	**browser_path:** *(Requerido, Tipo String)* Ruta del ejecutable Firefox.exe del servidor o equipo local necesario para levantar el Web Browser.
*	**url:** *(Requerido, Tipo String)* Dirección Web con la que vamos a mediante el Web Browser.
* Hidden: (Opcional, Tipo Booleano) Indica si el Web Browser se oculta durante su ejecución. False predeterminado.

**Ejemplo:**
```python
import libgal

browser=libgal.firefox(webdriver_path=r"C:\webdrivers\geckodriver.exe",browser_path=r"C:\Program Files\Mozilla Firefox\firefox.exe",url="https://bolsar.info/Cauciones.php")
```

[Volver al inicio](#selenium-web-browser-firefox)
