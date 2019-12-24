"""Provides classes:
    DataRaw
"""

from core.base.common import load_module, make_module_name
from .data import Data

class DataRaw(Data):
    """ Provides reading/writing data from/to raw data files (bin, netcdf, xml, ascii...).
    """

    def __init__(self, data_info):
        super().__init__(data_info)
        self._data_info = data_info
        data_class_name = 'Data' + data_info['data']['file']['@type'].capitalize()
        module_name = make_module_name(data_class_name)
        data_class = load_module(module_name, data_class_name, package_name=self.__module__)
        self._data = data_class(data_info)

    def read(self, options):
        """Reads raw data file into an array.

        Arguments:
            options -- dictionary of read options

        Returns:
            result['array'] -- data array
        """
        raise NotImplementedError


    def write(self, values, options):
        """Writes data (and metadata) to an output data file.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options

        """

        self._data.write(values, options)
