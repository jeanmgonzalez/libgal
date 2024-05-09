#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import datetime
from typing import Optional
import unidecode

DEFAULT_FILE_BUFFER_SIZE = 1024 * 1024  # 1 MB


class SingletonType(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                SingletonType, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class BufferingHandler(logging.Handler):
    def __init__(self, filename, encoding='utf-8', buffer_size=DEFAULT_FILE_BUFFER_SIZE):
        super().__init__()
        self.buffer_size = buffer_size
        self.filename = filename
        self.encoding = encoding
        self.fp = open(self.filename, mode='at', encoding=self.encoding)
        self.buffer = []

    def emit(self, record):
        msg = self.format(record)
        self.buffer.append(msg)

        if len(''.join(self.buffer)) >= self.buffer_size:
            self.flush()

    def flush(self):
        if self.buffer:
            log_entry = '\n'.join(self.buffer)
            self.fp.write(log_entry)
            self.buffer = []

    def close(self):
        self.flush()
        self.fp.write('\n')
        self.fp.close()
        super().close()

    @property
    def baseFilename(self):
        return self.filename


class Logger(object, metaclass=SingletonType):

    _logger = None

    def __init__(
            self,
            format_output: Optional[str] = None,
            app_name: Optional[str] = __name__,
            dirname: Optional[str] = None,
            level: Optional[int] = logging.DEBUG
    ):
        self._logger = logging.getLogger(app_name)

        self._logger.setLevel(level)
        self._id = id(self)
        self._app_name=app_name

        self.set_format(format_output)

        if dirname is not None:
            self.set_outputdir(dirname, format_output)

        self._logger.info(f"Generate new instance, hash = {self._id}")

    def __del__(self):
        logging.shutdown()

    def get_logger(self):
        return self._logger

    def get_id(self):
        return self._id

    def set_format(self, format_output: Optional[str]):
        formatter = None
        if format_output is not None and format_output.lower() == 'json':
            formatter = logging.Formatter(
                "{'time':'%(asctime)s', 'pid': '%(process)d', 'instance_hash': " 
                f"'{self._id}', "
                "'thread', '%(threadName)s', 'name': '%(name)s', 'level': '%(levelname)s', 'file': '%(filename)s', "
                "'lineno': %(lineno)s, 'message': '%(message)s'}",
                datefmt='%m/%d/%Y %I:%M:%S %p'
            )
        elif format_output is not None and format_output.lower() == 'csv':
            formatter = logging.Formatter(
                '%(asctime)s, %(process)d, '
                f"{self._id}, "
                '%(threadName)s, %(name)s, %(levelname)s, %(filename)s, %(lineno)s, "%(message)s"',
                datefmt='%m/%d/%Y %I:%M:%S %p'
            )
        else:
            formatter = logging.Formatter(
                f'%(asctime)s PID: %(process)d ({self._id}) %(threadName)s [%(levelname)s | %(filename)s:%(lineno)s] '
                f'> %(message)s '
            )

        found_stream_handler = False
        for handler in self._logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                found_stream_handler = True
            handler.setFormatter(formatter)

        if not found_stream_handler:
            streamHandler = logging.StreamHandler()
            streamHandler.setFormatter(formatter)
            self._logger.addHandler(streamHandler)

        return formatter

    def set_outputdir(self, dirname: Optional[str], log_format: Optional[str] = None):
        if dirname is not None:
            if not os.path.isdir(dirname):
                os.mkdir(dirname)
            formatter = self.set_format(log_format)
            now = datetime.datetime.now()
            for handler in self._logger.handlers:
                if isinstance(handler, BufferingHandler):
                    handler.close()
                    self._logger.removeHandler(handler)

            file_handler = BufferingHandler(
                dirname + f"/{unidecode.unidecode(self._app_name).replace(' ','_').lower()}_{os.getpid()}_" + now.strftime("%Y-%m-%d") + ".log", encoding='utf-8')
            file_handler.setFormatter(formatter)
            self._logger.addHandler(file_handler)
        else:
            self._logger.warning("No se ha especificado un directorio de salida para los logs")


# a simple usecase
if __name__ == "__main__":
    logger = Logger.__call__().get_logger()
    logger.info("Hello, Logger")
    logger.debug("bug occured")
