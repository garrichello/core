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

def listify(obj):
    """Makes a list from an object if it is not already a list.
    None is unchanged.

    Arguments:
        obj -- object of any type except list.

    Returns:
        result -- obj if is a list, [obj] otherwise.
    """
    result = obj
    if result is not None:
        if not isinstance(obj, list):
            result = [obj]
    
    return result