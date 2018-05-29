"""Provides classes
    DataNetcdf
"""

from datetime import datetime
from netCDF4 import MFDataset, date2index
from string import Template
import re

from base.common import listify

class PercentTemplate(Template):
    delimiter = '%'
    pattern = r'''
    \%(?:
        (?P<escaped>%) |
        (?P<named>[_a-z][_a-z0-9]*)% |
        \b\B(?P<braced>) |
        (?P<invalid>)
    )
    '''

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
        
        # Process each vertical level separately.
        for level_name in self._data_info["levels"]:
            file_name_template = self._data_info["levels"][level_name]["@file_name_template"]
            percent_template = PercentTemplate(file_name_template)
            file_name_wildcard = percent_template.substitute({"year" : "????"})
            netcdf_root = MFDataset("file_name_wildcard")
            # Process each time segment separately.
            for segment in self.segments:
            
                segment_start = datetime.strptime(segment["@beginning"], "%Y%m%d%H")
                segment_end = datetime.strptime(segment["@ending"], "%Y%m%d%H")
                time_idx_range = date2index([segment_start, segment_end], time_var)
                pass
        print(self._data_info)