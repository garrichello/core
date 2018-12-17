""" Unit testing module """

import unittest
from argparse import Namespace

from base.main_app import MainApp


class MainAppTest(unittest.TestCase):
    ''' Tests main application class'''

    def test_pass_taskfile_as_argument(self):
        '''Passing task file name as argparse arguments list should create a MainApp object'''
        args = Namespace(task_file_name='./tasks/CalcTimean.tmpl.xml')

        resut = MainApp(args)
        self.assertTrue(isinstance(resut, MainApp))

if __name__ == '__main__':
    unittest.main()
