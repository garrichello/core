"""Provides classes
    DataNetcdf
"""

import datetime
from dateutil import parser

from base.common import listify

class DataNetcdf:
    def __init__(self, data_info):
        self._data_info = data_info

    def read(self, segments, levels):
        """Reads netCDF-file into an array.

        Arguments:
            segments -- time segments
            levels -- vertical levels

        Returns:
            result["array"] -- data array
        """

        # Segments must be a list or None.
        self._segments = listify(segments)
        self._levels = listify(levels)
        

        print(self._data_info)