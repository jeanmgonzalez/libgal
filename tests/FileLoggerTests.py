import unittest
from libgal.modules.Logger import Logger


class FileLoggerTests(unittest.TestCase):

    def test_standard_log(self):
        logger = Logger().get_logger()
        logger.info('Test INFO standard')


if __name__ == '__main__':
    unittest.main()
