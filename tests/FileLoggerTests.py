import logging
import unittest
from libgal.modules.Logger import Logger, BufferingHandler
import os


class FileLoggerTests(unittest.TestCase):

    def test_standard_log(self):
        logger = Logger(dirname='./logs').get_logger()
        logger.info('Testing File Logger')
        # checks that the log file was created
        for handler in logger.handlers:
            if isinstance(handler, BufferingHandler):
                filename = handler.baseFilename
                assert os.path.isfile(filename), 'El archivo de log no fue creado'
                logger.info(f'Log file: {filename} will be deleted now')
                logging.shutdown()
                os.remove(filename)
                os.rmdir(os.path.dirname(filename))




if __name__ == '__main__':
    unittest.main()
