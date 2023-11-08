#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import datetime
from typing import Optional


class SingletonType(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                SingletonType, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class BufferingHandler(logging.Handler):
    def __init__(self, filename, encoding='utf-8', buffer_size=1024 * 1024):  # Default buffer size is 1 MB
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


class Logger(object, metaclass=SingletonType):

    _logger = None

    def __init__(
            self,
            format_output: Optional[str] = None,
            app_name: Optional[str] = __name__,
            dirname: Optional[str] = "./logs"
    ):
        self._logger = logging.getLogger(app_name)

        ##Cierra las conexiones de logueo activos Handler
        for handler in self._logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

        self._logger.setLevel(logging.DEBUG)
        self._id = id(self)

        if format_output is not None and format_output.lower() == 'json':
            formatter = logging.Formatter(
                "{'time':'%(asctime)s', 'name': '%(name)s','level': '%(levelname)s', 'message': '%(message)s'}",
                datefmt='%m/%d/%Y %I:%M:%S %p')
        elif format_output is not None and format_output.lower() == 'csv':
            formatter = logging.Formatter('%(asctime)s;%(name)s;%(levelname)s;%(message)s',
                                         datefmt='%m/%d/%Y %I:%M:%S %p')
        else:
            formatter = logging.Formatter(
                f'%(asctime)s PID: %(process)d ({self._id}) %(threadName)s [%(levelname)s | %(filename)s:%(lineno)s] > %(message)s'
            )

        now = datetime.datetime.now()

        if dirname is not None:
            if not os.path.isdir(dirname):
                os.mkdir(dirname)

            fileHandler = BufferingHandler(
                dirname + "/log_" + now.strftime("%Y-%m-%d") + ".log", encoding='utf-8')

            fileHandler.setFormatter(formatter)
            self._logger.addHandler(fileHandler)

        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)

        self._logger.addHandler(streamHandler)
        self._logger.info(f"Generate new instance, hash = {self._id}")

    def __del__(self):
        logging.shutdown()

    def get_logger(self):
        return self._logger

    def get_id(self):
        return self._id


# a simple usecase
if __name__ == "__main__":
    logger = Logger.__call__().get_logger()
    logger.info("Hello, Logger")
    logger.debug("bug occured")
