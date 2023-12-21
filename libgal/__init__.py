# -*- coding: utf-8 -*-
"""
Created on Fri Mar 25 15:22:35 2022

@author: Jean Manuel González Mejía
@version: 0.0.13
@Description: Librería para la simplificación del código en proyectos de Python
@last_update: 2023-06-22
"""

try:

    import logging  # Libreria para logs
    import os
    from pathlib import Path

    # Selenium
    from selenium import webdriver
    from selenium.webdriver.firefox.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.select import Select

    from bs4 import BeautifulSoup

    # Teradata
    import teradatasql
    from libgal.modules.Teradata import teradata
    from teradatasql import OperationalError as TeradataError

    # Variables de entorno
    from dotenv import load_dotenv

    # SQLALchemy
    from sqlalchemy import create_engine, Column, Integer, String, text, and_
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.exc import OperationalError as SQLAlchemyError

    # Machine Learning
    from sklearn.base import BaseEstimator, TransformerMixin
    import numpy as np
    import pandas
    from collections import defaultdict
    from scipy.stats import ks_2samp
    from sklearn.metrics import roc_curve, roc_auc_score
    from libgal.modules.Logger import Logger
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.common.action_chains import ActionChains


except ImportError as imp_err:
    # Freno ejecucion y devuelvo codigo de error
    raise ImportError(f"Error al importar libreria: {imp_err}")


def variables_entorno(path_env_file=None):
    """
    Descripción: Toma las variables de entorno del archivo .env o del SO
    Parámetro:
    - path_env_file (String):
    """

    if path_env_file != None and Path(path_env_file).exists():

        load_dotenv(path_env_file)

    else:

        print(
            f"No se encontró el archivo {path_env_file} indicado para funcion variables_entorno() de libgal por lo que se toma las variables de entorno de sistema.")

    return dict(os.environ)


class LoggerFormatException(Exception):
    pass


def logger(format_output="JSON", app_name=__name__):
    """
    Descripción: Crea un nuevo logger
    Parámetro:
    - format_output (String): Tipo de Salida del Log (JSON, CSV)
    - app_name (String): Nombre de la aplicación para el log
    """
    if format_output not in ['CSV', 'JSON']:
        raise LoggerFormatException("Tipo de formato de Log inválido. Formatos soportados (JSON y CSV).")
    # Create a custom logger
    logger = Logger(format_output=format_output, app_name=app_name, dirname=None).get_logger()
    logger.setLevel(logging.INFO)

    return logger


def shutdown_logger():
    """
    Descripción: Cierra el log

    """
    pass


def firefox(webdriver_path, browser_path, url, hidden=False, tipo_archivo=None, ruta_descarga=None):

    """
    Descripción: Crea un cliente web para pruebas, scrapings y automatizaciones
    Parámetro:
    - webdriver_path (String): Path completo de la ruta y el archivo ejecutable del driver para el cliente web
    - browser_path (String): Path completo de la ruta y el archivo ejecutable del browser web
    - url (String): URL del sitio web a explorar.
    - hidden (Boolean): Indica si se oculta o no el cliente web. False por defecto.
    """

    options = webdriver.FirefoxOptions()
    options.binary_location = browser_path

    if hidden:
        options.add_argument("--headless")

    #profile = webdriver.FirefoxProfile()
    options.set_preference('browser.download.folderList', 2)
    options.set_preference('browser.download.manager.showWhenStarting', False)

    if ruta_descarga:
        options.set_preference('browser.download.dir', str(ruta_descarga))
        #options.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/octet-stream')

    if tipo_archivo.lower()=='pdf':
        options.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/pdf')
        options.set_preference('pdfjs.disabled',True)
    elif tipo_archivo.lower()=='txt':
        options.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/plain')
    elif tipo_archivo.lower()=='png':
        options.set_preference('browser.helperApps.neverAsk.saveToDisk', 'image/png')
    elif tipo_archivo.lower()=='jpg':
        options.set_preference('browser.helperApps.neverAsk.saveToDisk', 'image/jpeg')


    driver_service=Service(webdriver_path)

    web_browser = webdriver.Firefox(service=driver_service, options=options)
    web_browser.get(url)

    return web_browser


def html_parser(html):
    """
    Descripción: Parsea el código HTML para encontrar etiquetas específicas
    Parámetro:
    - html (String): código html a parsear
    """

    soup = BeautifulSoup(html, 'html.parser')

    return soup


class sqlalchemy:
    """
    Descripción: Permite la conexion hacia la Base de Datos
    Parámetros:
    - driver (String): Tipo de conexión o base de datos a utilizar
    - host (String): uri del servidor de base de datos
    - username (String): Usuario que autentica la conexión a la base de datos
    - password (String): Contraseña para la autenticación de la connexión de la base de datos
    - logmech (String): Parámetro Opcional que indica el método de autenticación del usuario. LDAP por defecto
    """

    def __init__(self, driver, host, username, password, logmech="LDAP", timeout_seconds=None, pool_recycle=1800,
                 pool_size=20):

        if driver.lower() == "teradata":
            if timeout_seconds:
                self.engine = create_engine(f"teradatasql://{username}:{password}@{host}/?logmech={logmech.upper()}",
                                            pool_recycle=pool_recycle, pool_size=pool_size,
                                            connect_args={'connect_timeout': timeout_seconds})
            else:
                self.engine = create_engine(f"teradatasql://{username}:{password}@{host}/?logmech={logmech.upper()}",
                                            pool_recycle=pool_recycle, pool_size=pool_size)
        elif driver.lower() == "mysql":
            self.engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}/",
                                        pool_recycle=pool_recycle, pool_size=pool_size)

    def Session(self):
        self.Session = sessionmaker(bind=self.engine)
        return self.Session()

    def Base(self):
        return declarative_base()

    def query(self, query):
        """
        Descripción: Permite ejecutar una instrucción SQL según el motor de Base de Datos.
        Parámetro:
        - query (String): Instrucción SQL a ejecutar
        """
        with self.engine.connect() as conn:
            return conn.execute(text(query))

    def InsertDataframe(self, pandas_dataframe, database, table):

        """
        Descripción: Permite ejecutar una instrucción SQL según el motor de Base de Datos.
        Parámetro:
        - pandas_dataframe: Dataframe de Pandas que contiene la info a insertar
        - database (String): Base de datos que contiene la tabla a poblar.
        - table (String): Tabla donde se insertaran los datos del Dataframe
        """

        with self.engine.connect() as conn:

            pandas_dataframe = pandas_dataframe.astype(str)

            try:

                pandas_dataframe.to_sql(table, schema=database, con=conn, if_exists='append', index=False)

            except SQLAlchemyError as e:
                print(e)

