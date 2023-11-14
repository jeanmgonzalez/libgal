# -*- coding: utf-8 -*-
"""
Created on Fri Mar 25 15:22:35 2022

@author: Jean Manuel González Mejía
@version: 0.0.13
@Description: Librería para la simplificación del código en proyectos de Python
@last_update: 2023-06-22
"""

try:

    import logging #Libreria para logs
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
    from teradatasql import OperationalError as TeradataError

    # Variables de entrono
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


def firefox(webdriver_path, browser_path, url, hidden=False):
    """
    Descripción: Crea un cliente web para pruebas y automatizaciones
    Parámetro:
    - format_output (String): Tipo de Salida del Log (JSON, CSV)
    - app_name (String): Nombre de la aplicación para el log
    """

    options = webdriver.FirefoxOptions()
    options.binary_location = browser_path
    options.headless = hidden

    driver_service = Service(webdriver_path)

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


def teradata(host, username, password, logmech="LDAP", database=None):
    # Funcion que permite la conexion hacia el sgdb

    """
    Descripción: Permite la conexion hacia la Base de Teradata
    Parámetros:
    - host (String): uri del servidor de base de datos
    - username (String): Usuario que autentica la conexión a la base de datos
    - password (String): Contraseña para la autenticación de la connexión de la base de datos
    - logmech (String): Parámetro Opcional que indica el método de autenticación del usuario. LDAP por defecto
    - database (String): Parámetro Opcional que indica la base de datos a la cual nos vamos a conectar
    """
    if database:
        td_connection = teradatasql.connect(host=host, user=username, password=password, logmech=logmech.upper(),
                                            database=database)
    else:
        td_connection = teradatasql.connect(host=host, user=username, password=password, logmech=logmech.upper())

    return td_connection


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


####FUNCIONES DE MACHINE LEARNING

def evaluate_ks_and_roc_auc(y_real, y_proba):
    # Unite both visions to be able to filter
    df = pandas.DataFrame()
    df['real'] = y_real
    df['proba'] = y_proba
    class0 = df[df['real'] == 0]
    class1 = df[df['real'] == 1]
    ks = ks_2samp(class0['proba'], class1['proba'])
    roc_auc = roc_auc_score(df['real'], df['proba'])

    print(f"KS: {ks.statistic:.4f} (p-value: {ks.pvalue:.3e})")
    print(f"ROC AUC: " + str(roc_auc))
    return ks.statistic, roc_auc, df


class NumNormTransformer(BaseEstimator, TransformerMixin):
    # the constructor
    def __init__(self, keep_original=True, sufix=['vl'], exclude=['periodo_cd', 'cd_periodo']):
        self.keep_original = keep_original
        self.sufix = sufix
        self.exclude = exclude

    # estimator method
    def fit(self, X, y=None):
        return self

    # transfprmation
    def transform(self, X, y=None):
        self.originals = []
        for col in X.columns:
            for suf in self.sufix:
                if (col.endswith(suf) or col.startswith(suf)) and col not in self.exclude:
                    self.originals.append(col)

        self.X_norm = X[self.originals]
        self.X_norm.columns = [str(x) + "_norm" for x in self.X_norm]
        self.X_norm = (self.X_norm - self.X_norm.mean()) / (self.X_norm.std())

        self.X_sca = X[self.originals]
        self.X_sca.columns = [str(x) + "_sca" for x in self.X_sca]
        self.X_sca = (self.X_sca.max() - self.X_sca) / (self.X_sca.max() - self.X_sca.min())
        X.drop(columns=self.originals, inplace=True)

        X = pandas.concat([X, self.X_norm], axis=1)
        return X


class NumLogTransformer(BaseEstimator, TransformerMixin):
    # the constructor
    def __init__(self, keep_original=True, sufix=['vl'], exclude=['periodo_cd', 'cd_periodo']):
        self.keep_original = keep_original
        self.sufix = sufix
        self.exclude = exclude

    # estimator method
    def fit(self, X, y=None):
        return self

    # transfprmation
    def transform(self, X, y=None):
        self.originals = []
        for col in X.columns:
            for suf in self.sufix:
                if (col.endswith(suf) or col.startswith(suf)) and col not in self.exclude:
                    self.originals.append(col)

        self.X_log = X[self.originals]
        self.X_log.columns = [str(x) + "_log" for x in self.X_log]
        self.X_log = np.log(self.X_log + 1)
        X = pandas.concat([X, self.X_log], axis=1)
        if not self.keep_original:
            X.drop(columns=self.originals, inplace=True)
        return X


class CategoricalReduceTransformer(BaseEstimator, TransformerMixin):
    # the constructor
    def __init__(self, keep_original=False, sufix=['tx', 'cd'], threshold=0.99, exclude=['periodo_cd', 'cd_periodo']):
        self.keep_original = keep_original
        self.sufix = sufix
        self.threshold = threshold
        self.exclude = exclude

    # estimator method
    def fit(self, X, y=None):
        return self

    # transfprmation
    def transform(self, X, y=None):
        self.originals = []
        for col in X.columns:
            for suf in self.sufix:
                if (col.endswith(suf) or col.startswith(suf)) and col not in self.exclude:
                    self.originals.append(col)
                    D = X.groupby([col], as_index=False).size().rename(columns={'size': 'n'}).sort_values('n',
                                                                                                          ascending=False)
                    D['p'] = D['n'] / D['n'].sum()
                    D['cumsum_p'] = D['p'].cumsum()
                    dict_estados = defaultdict(lambda: 'Otros')
                    for i in range(len(D)):
                        if D.cumsum_p[i] < self.threshold:
                            dict_estados[D[col][i]] = str(D[col][i]).replace(".", "").replace("°", "").replace(">",
                                                                                                               " ").replace(
                                "<", " ").replace("/", " ")
                        else:
                            dict_estados[D[col][i]] = "Otros"
                    if self.keep_original:
                        X["freq_" + str(col)] = [dict_estados[x] for x in X[col]]
                    else:
                        X[str(col)] = [dict_estados[x] for x in X[col]]

        return X
