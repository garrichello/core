"""Provides classes:
    Proc
"""

from base.dataaccess import DataAccess
from base.common import load_module

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
        self._data_helper = DataAccess(inputs, outputs, metadb_info)

    def run(self):
        """Creates an instance of the processing module class and runs it
        """
        # Let's try to create an instance of the processing class.
        proc_class = load_module('mod', self._proc_class_name)
        processor = proc_class(self._data_helper)

        # And here we run the module.
        try:
            processor.run()
        except AttributeError:
            print('(Proc::run) No method \'run\' in the class ' + self._proc_class_name)
            raise