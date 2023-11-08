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
        logger = Logger('None', __name__)
        logger.info('Test INFO None')


if __name__ == '__main__':
    unittest.main()
