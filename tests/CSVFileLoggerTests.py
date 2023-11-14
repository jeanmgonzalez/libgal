import unittest
from libgal.modules.Logger import Logger


class FileLoggerTests(unittest.TestCase):

    def test_csv_log(self):
        logger = Logger('CSV').get_logger()
        logger.info('Test info .CSV')


if __name__ == '__main__':
    unittest.main()
