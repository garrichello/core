"""Common functions and classes"""

import importlib
import datetime
import os.path
# Python 3 only
import builtins

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
            print('(MainApp::load_module) Class ' + class_name + ' does not exist')
            raise
    except ImportError:
        print('(MainApp::load_module) Module ' + module_name + ' does not exist')
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

def print(*args, **kwargs):
    """Prints out to a standard output a string prefixed with current date and time

    Arguments:
        string_ -- string to print

    """
    now = datetime.datetime.now()
    date_time = '({0:02}/{1:02}/{2:04} {3:02}:{4:02}:{5:02}) '.format(now.day, now.month, now.year, now.hour, now.minute, now.second)
    builtins.print(date_time, end='')
    return builtins.print(*args, **kwargs)

def make_filename(data_info, options):
    """Constructs a file name for writing output.
    It's used in write-methods of various data-access classes.

    Arguments:
        data_info -- dictionary describing dataset, usually it's a self._data.info in a data-access module.
        options -- dictionary containing metadata of the data array to be written, usually it's a options argument of the write method.

    Returns:
        filename -- constructed filename which includes level name and time segment.

    """
    (file_root, file_ext) = os.path.splitext(data_info['data']['file']['@name'])
    filename = '{}_{}_{}-{}{}'.format(file_root, options['level'], 
            options['segment']['@beginning'], options['segment']['@ending'], file_ext)
            
    return filename