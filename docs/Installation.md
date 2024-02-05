## Instrucciones de instalación

### Requisitos previos

- [Python](https://www.python.org/downloads/) 3.7 o versión superior
- git [Git-SCM](https://git-scm.com/)
- IDE [PyCharm Community Edition](https://www.jetbrains.com/pycharm/download/), [VSCode](https://code.visualstudio.com/) o su favorito con soporte para debug de Python (para desarrollo, no necesario para ejecución) 
- virtualenv (opcional)

### Instalación desde pypi

```bash
pip install libgal
```

### Instalación en un entorno virtual
    
```bash
cd /path/to/project
python -m venv venv
source venv/bin/activate # en Windows: venv\Scripts\activate
pip install libgal
```

### Notas de instalación en Windows

Para instalar en Windows, es necesario tener instalado el compilador de C++ de Microsoft.
Se puede descargar desde [Visual Studio](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
y seleccionar la opción "C++ build tools" en la instalación.

También se puede compilar con el compilador de C++ [mingw](http://mingw-w64.org), por [Cygwin](https://cygwin.com/), o por la consola de [MobaXterm personal edition](https://mobaxterm.mobatek.net/download.html) luego de instalar el paquete gcc-core.

En Cygwin, se instala con el siguiente comando:
```bash
apt-cyg install gcc-core
```

En MobaXterm:
```bash
apt install gcc-core
```

### Notas de instalación en Linux
Tiene que estar instalado el compilador de C++ de GNU.

Debian/Ubuntu/Mint
```bash
sudo apt-get install build-essential
```

CentOS/RHEL/Fedora
```bash
sudo yum groupinstall 'Development Tools'
```

Arch/Manjaro
```bash
sudo pacman -S base-devel
```

### Instalación desde el código fuente

```bash
git clone https://github.com/jeanmgonzalez/libgal.git
cd libgal
pip install .
```

### Pruebas

Las pruebas se pueden realizar ejecutando los tests del directorio [tests](../tests) situado en el directorio raíz del proyecto. 

Por ejemplo:
```bash
cd /path/to/libgal
cd tests
python -m unittest TeradataMLTests.py
```
Dicho test generará un DataFrame con datos aleatorios y lo insertará en una tabla de Teradata.

El test es interactivo y solicitará los datos de conexión a la base de datos.

### Desinstalación

```bash
pip uninstall libgal
```

