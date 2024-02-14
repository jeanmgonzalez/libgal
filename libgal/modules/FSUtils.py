import os
from os import listdir, stat, remove, chdir
from os.path import isfile, join, dirname
from pathlib import Path
from time import time

from libgal.modules.Logger import Logger

logger = Logger(dirname=None).get_logger()

OUTPUT_DIRS = ['output', 'logs']
DB_DIR = ['db']


def delete_older_files(path: str, max_days: int = 30, dry_run: bool = False, odate = None):
    """
    Elimina los archivos de un directorio que superen la antigüedad máxima
        :param path: Directorio a limpiar
        :param max_days: Antigüedad máxima en días
        :param dry_run: Si es True, no elimina los archivos, solo muestra los que eliminaría
        :param odate: Fecha de referencia para calcular la antigüedad de los archivos
    """
    for f in listdir(path):
        filepath = join(path, f)
        age_max = max_days * 86400
        t = time() if odate is None else odate.timestamp()
        fileage = t - stat(filepath).st_mtime
        if fileage > age_max and isfile(filepath):
            age_days = round(fileage / 86400)
            logger.info('Eliminando %s, (%d días de antigüedad)' % (filepath, age_days))
            if not dry_run:
                try:
                    remove(filepath)
                except Exception as e:
                    logger.error('Error %s' % e)
                    pass
            else:
                logger.info('Omitiendo acción (dry run) en %s' % filepath)


def create_dirs(dir_list):
    """
    Crea los directorios de la lista (si no existen)
        :param dir_list: Lista de directorios a crear
    """
    for dir_ in dir_list:
        logger.info('Creando directorio %s' % dir_)
        Path(dir_).mkdir(parents=True, exist_ok=True)


def change_to_public_permissions(path):
    """
    Cambia los permisos de los archivos de un directorio a 664
        :param path: Directorio a cambiar permisos
    """
    for f in listdir(path):
        filepath = join(path, f)
        if isfile(filepath):
            try:
                mode = 0o664
                logger.info('Cambiando permisos de %s a %s' % (filepath, oct(mode)))
                Path(filepath).chmod(mode)
            except Exception as e:
                logger.error('Error %s' % e)
                pass


def create_output_dirs(home):
    """
    Crea los directorios de salida
        :param home: Directorio raíz
    """
    chdir(dirname(home))
    all_dirs = OUTPUT_DIRS + DB_DIR
    create_dirs(all_dirs)
    for dir_ in all_dirs:
        logger.info('Cambiando permisos de %s' % dir_)
        change_to_public_permissions(dir_)


def delete_files(path, dry_run=False):
    """
    Elimina los archivos de un directorio (si existen)
        :param path: Directorio a limpiar
        :param dry_run: Si es True, no elimina los archivos, solo muestra los que eliminaría
    """
    for f in listdir(path):
        filepath = join(path, f)
        if isfile(filepath):
            logger.info('Eliminando %s' % filepath)
            if not dry_run:
                try:
                    remove(filepath)
                except Exception as e:
                    logger.error('Error %s' % e)
                    pass
            else:
                logger.info('Omitiendo acción (dry run) en %s' % filepath)


def init_env(home):
    """
    Inicializa el entorno de ejecución
        :param home: Directorio raíz
    """
    os.chdir(home)
    create_output_dirs(home)
    for dir_ in OUTPUT_DIRS:
        delete_older_files(dir_, max_days=90)
    change_to_public_permissions('logs')
    delete_files(Path('output'))
