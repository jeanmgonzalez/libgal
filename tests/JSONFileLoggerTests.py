import unittest
from libgal.modules.Logger import Logger


class FileLoggerTests(unittest.TestCase):

    def test_json_log(self):
        logger = Logger('JSON').get_logger()
        logger.info('Test INFO .JSON')

if __name__ == '__main__':
    unittest.main()
