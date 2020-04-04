"""Common functions and classes"""

import importlib
import os.path
import logging

ZERO_CELSIUS_IN_KELVIN = 273.15  # 0 degC is 273.15 degK
MOD_PACKAGE_PATH = 'mod'
CALC_SUBPACKAGE_NAME = 'calc'
DATA_SUBPACKAGE_NAME = 'data'
CVC_PREFIX = 'cvc'
CALC_PREFIX = 'calc'
DATA_PREFIX = 'data'
logger = logging.getLogger()

def celsius_to_kelvin(temperature_in_celsius):
    """Converts temperature in Celsius to Kelvin

    Arguments:
        temperature_in_celsius -- temperature in degC

    Returns temperature in degK
    """
    return temperature_in_celsius + ZERO_CELSIUS_IN_KELVIN

def kelvin_to_celsius(temperature_in_kelvin):
    """Converts temperature in Kelvin to Celsius

    Arguments:
        temperature_in_kelvin -- temperature in degK

    Returns temperature in degC
    """
    return temperature_in_kelvin - ZERO_CELSIUS_IN_KELVIN

def load_module(module_name, class_name, package_name=None):
    """Loads module by its name and returns class to instantiate

    Arguments:
        module_name -- name of the Python module (file name)
        class_name -- name of the class in this module
        package_name -- (optional) name of the module's package (for relative module naming)
    """
    relative_shift = '' if package_name is None else '.'*len(package_name.split('.'))
    load_module_name = relative_shift + module_name
    try:
        module_ = importlib.import_module(load_module_name, package=package_name)
        try:
            class_ = getattr(module_, class_name)
        except AttributeError:
            logger.error('Class %s does not exist', class_name)
            raise
    except ImportError:
        logger.error('Module %s does not exist', load_module_name)
        raise
    return class_

def make_module_name(class_name):
    """Makes a relative module name based on the name of the class

    Arguments:
        class_name -- name of the class
    """
    module_name = class_name.lower().split(CVC_PREFIX)[-1]  # Remove prefix 'cvc' if present to get module's name.
    if module_name[0:4] == CALC_PREFIX:
        module_name = '.'.join([CALC_SUBPACKAGE_NAME, module_name])
    if module_name[0:4] == DATA_PREFIX:
        module_name = '.'.join([DATA_SUBPACKAGE_NAME, module_name])
    module_name = '.'.join([MOD_PACKAGE_PATH, module_name])

    return module_name

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
            if not list_:
                result = None
            else:
                result = list_[0]
        else:
            result = list_
    else:
        result = None
    return result


def list_remove_all(list_, item_to_remove):
    """ Removes all occurences of the item_to_remove from the list """

    return [item for item in list_ if item != item_to_remove]


def make_filename(data_info, all_options):
    """Constructs a file name for writing raw output.
    It's used in write-methods of various data-access classes.
    Names of vertical levels are included into the filename.

    Arguments:
        data_info -- dictionary describing dataset, usually it's a self._data.info in a data-access module.
        options -- dictionary containing metadata of the data array to be written, usually it's an options
        argument of the write method.

    Returns:
        filename -- constructed filename which includes level name and time segment.

    """
    if isinstance(all_options, list):
        options = all_options[0]
    else:
        options = all_options
#    (file_root, file_ext) = os.path.splitext(data_info['data']['file']['@name'])
#    filename = '{}_{}_{}-{}{}'.format(file_root, options['level'], options['segment']['@beginning'],
#                                      options['segment']['@ending'], file_ext)
    filename = data_info['data']['file']['@name']

    return filename

def make_raw_filename(data_info, all_options):
    """Constructs a file name for writing raw output.
    It's used in write-methods of various data-access classes.
    Names of vertical levels are NOT included into the filename.

    Arguments:
        data_info -- dictionary describing dataset, usually it's a self._data.info in a data-access module.
        options -- dictionary containing metadata of the data array to be written, usually it's an options
        argument of the write method.

    Returns:
        filename -- constructed filename which includes level name and time segment.

    """
    if isinstance(all_options, list):
        options = all_options[0]
    else:
        options = all_options
#    (file_root, file_ext) = os.path.splitext(data_info['data']['file']['@name'])
#    filename = '{}_{}-{}{}'.format(file_root, options['segment']['@beginning'],
#                                   options['segment']['@ending'], file_ext)
    filename = data_info['data']['file']['@name']

    return filename
