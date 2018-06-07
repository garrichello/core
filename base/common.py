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

def unlistify(list_):
    """Extracts an object from a list if it is a list. 
    None is unchanged.

    Arguments:
        list_ -- object of type list

    Returns: list_[0]. None if list_ is empty. Raises ValueError if more than one element is in list_.
    """
    if list_ is not None:
        if isinstance(list_, list):
            if len(list_) == 0:
                result = None
            elif len(list_) == 1:
                result = list_[0]
            else:
                raise ValueError
    else:
        result = None
    return result