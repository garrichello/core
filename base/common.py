"""Common functions and classes"""

import importlib

def load_module(module_name, class_name):
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
            raise
    except ImportError:
        print("(MainApp::load_module) Module " + module_name + " does not exist")
        raise
    return class_