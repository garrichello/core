"""Class Proc. 
Loads a processing module and runs it providing a corresponding data access API and error handling
"""

import importlib

from base.dataaccess import DataAccess

class Proc:
    def __init__(self, inputs, outputs, metadb_info):
        """Creates an instance of a class-helper that provides the data access API for modules
        
        Arguments:
            inputs -- list of dictionaries describing input arguments of the processing module
            outputs -- list of dictionaries describing output arguments of the processing module
            metadb_info -- dictionary describing metadata database (location and user credentials)
        """
        self._data_helper = DataAccess(inputs, outputs, metadb_info)

    def _load_module(self, module_name, class_name):
        """Loads module by its name and returns class to instantiate

        Arguments:
            module_name -- name of the Python module (file name)
            class_name -- name of the class in this module
        """
        try:
            module_ = importlib.import_module(module_name)
            try:
                class_ = getattr(module_, class_name)
            except AttributeError:
                print("(MainApp::load_module) Class " + class_name + " does not exist")
        except ImportError:
            print("(MainApp::load_module) Module " + module_name + " does not exist")
        return class_ or None

    def run(self, proc_class_name):
        """Creates an instance of the processing module class and runs it
        
        Arguments:
            class_name -- name of the processing class
        """
        # let's try to create an instance of the processing class
        proc_class = self._load_module("mod", proc_class_name)
        processor = proc_class(self._data_helper)

        # and here we run the module
        try:
            processor.run()
        except AttributeError:
            print("(Proc::run) No method 'run' in the class " + proc_class_name)
            raise