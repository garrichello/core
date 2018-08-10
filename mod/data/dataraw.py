"""Provides classes:
    DataRaw
"""

from base.common import load_module

class DataRaw:
    """ Provides reading/writing data from/to raw data files (bin, netcdf, xml, ascii...).
    """

    def __init__(self, data_info):
        self._data_info = data_info
        data_class_name = 'Data' + data_info['data']['file']['@type'].capitalize()
        data_class = load_module('mod', data_class_name)
        self._data = data_class(data_info)

    def read(self, options):
        """Reads raw data file into an array.

        Arguments:
            options -- dictionary of read options

        Returns:
            result['array'] -- data array
        """
        
        pass    

    def write(self, values, options):
        """Writes data (and metadata) to an output data file.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options

        """
        
        self._data.write(values, options)
