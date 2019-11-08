"""Provides classes:
    Proc
"""

import logging
from .dataaccess import DataAccess
from .common import load_module, make_module_name

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

        self.logger = logging.getLogger()
        self._proc_class_name = proc_class_name
        self.logger.info('Initializing processing module %s', proc_class_name)
        self._data_helper = DataAccess(inputs, outputs, metadb_info)
        self.logger.info('Done!')

    def run(self):
        """Creates an instance of the processing module class and runs it
        """
        # Let's try to create an instance of the processing class

        module_name = make_module_name(self._proc_class_name)
        proc_class = load_module(module_name, self._proc_class_name, package_name=self.__module__)
        processor = proc_class(self._data_helper)

        # And here we run the module.
        self.logger.info('Starting processing module %s...', self._proc_class_name)
        try:
            processor.run()
        except AttributeError:
            self.logger.error('No method \'run\' in the class %s', self._proc_class_name)
            raise
        self.logger.info('Processing module %s exited.', self._proc_class_name)
