"""Provides classes:
    Proc
"""

from .dataaccess import DataAccess
from .common import load_module, print

MOD_PACKAGE_RELPATH = '...mod'
CALC_SUBPACKAGE_NAME = 'calc'
CVC_PREFIX = 'cvc'
CALC_PREFIX = 'calc'

class Proc:
    """Class Proc.
    Loads a processing module and runs it providing a corresponding data access API and error handling
    """
    def __init__(self, proc_class_name, inputs, outputs, metadb_info):
        """Creates an instance of a class-helper that provides the data access API for modules

        Arguments:
            proc_class_name -- name of the processing class
            inputs -- list of dictionaries describing input arguments of the processing module
            outputs -- list of dictionaries describing output arguments of the processing module
            metadb_info -- dictionary describing metadata database (location and user credentials)
        """
        self._proc_class_name = proc_class_name
        print('(Proc::__init__) Initializing processing module {}'.format(proc_class_name))
        self._data_helper = DataAccess(inputs, outputs, metadb_info)
        print('(Proc::__init__) Done!')

    def run(self):
        """Creates an instance of the processing module class and runs it
        """
        # Let's try to create an instance of the processing class
        module_name = self._proc_class_name.lower().split(CVC_PREFIX)[-1]  # Remove prefix 'cvc' if present to get module's name.
        if module_name[0:4] == CALC_PREFIX:
            print('Calc module!')
            module_name = '.'.join([CALC_SUBPACKAGE_NAME, module_name])
        module_name = '.'.join([MOD_PACKAGE_RELPATH, module_name])
        proc_class = load_module(module_name, self._proc_class_name, package_name=self.__module__)
        processor = proc_class(self._data_helper)

        # And here we run the module.
        print('(Proc::run) Starting processing module {}...'.format(self._proc_class_name))
        try:
            processor.run()
        except AttributeError:
            print('(Proc::run) No method \'run\' in the class ' + self._proc_class_name)
            raise
        print('(Proc::run) Processing module {} exited.'.format(self._proc_class_name))
