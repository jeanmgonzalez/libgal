import libgal
from libgal import logger as Logger
import unittest


class LoggerTests(unittest.TestCase):

    def test_json(self):
        logger = Logger('JSON', __name__)
        logger.info('Test INFO json')

    def test_csv(self):
        logger = Logger('CSV', __name__)
        logger.info('Test INFO CSV')

    def test_other(self):
        passes = False
        try:
            logger = Logger(None, __name__)
        except libgal.LoggerFormatException as e:
            passes = True
        assert passes, 'Se espera que lance LoggerFormatException si no se provee un formato válido'


if __name__ == '__main__':
    unittest.main()
