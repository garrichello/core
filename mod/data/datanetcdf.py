"""Provides classes
    DataNetcdf
"""

from datetime import datetime
from netCDF4 import MFDataset, date2index
from string import Template
import re

from base.common import listify, unlistify

LONGITUDE_UNITS =  {"degrees_east", "degree_east", "degrees_E", "degree_E", "degreesE", "degreeE", "lon"}
LATITUDE_UNITS = {"degrees_north", "degree_north", "degrees_N", "degree_N", "degreesN", "degreeN", "lat"}
TIME_UNITS = {"since", "time"}

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

        variable_idxs = {} # Contains first and last index for each dimension of the data variable in the domain to be read
        
        # Process each vertical level separately.
        for level_name in self._data_info["levels"]:
            level_variable_name = self._data_info["levels"][level_name]["@level_variable_name"]
            file_name_template = self._data_info["levels"][level_name]["@file_name_template"] # Template as in MDDB.
            percent_template = PercentTemplate(file_name_template) # Custom string template %keyword%.
            file_name_wildcard = percent_template.substitute({"year" : "????"}) # Create wildcard-ed template

            netcdf_root = MFDataset(file_name_wildcard)

            longitude_variable = unlistify(netcdf_root.get_variables_by_attributes(units=lambda v: v in LONGITUDE_UNITS))
            if longitude_variable.ndim == 1:
                pass

            latitude_variable = unlistify(netcdf_root.get_variables_by_attributes(units=lambda v: v in LATITUDE_UNITS))

            # Determine index of the current vertical level to read data variable.
            if level_variable_name != "none":
                try:
                    level_variable = netcdf_root.variables[level_variable_name]
                except KeyError:
                    print ("(DataNetcdf::read) Level variable '{0}' is not found in files. Aborting!".format(
                            level_variable_name))
                    raise
                try:
                    level_index = level_variable[:].astype("str").tolist().index(level_name)
                except KeyError:
                    print ("(DataNetcdf::read) Level '{0}' is not found in level variable '{1}'. Aborting!".format(
                            level_name, level_variable_name))
                    raise                    
            else:
                level_index = None

            time_variable = unlistify(netcdf_root.get_variables_by_attributes(
                    units=lambda v: True in [tu in v for tu in TIME_UNITS]))

            data_variable = netcdf_root.variables[self._data_info["data"]["variable"]["@variable_name"]]
            # Process each time segment separately.
            for segment in self._segments:
                segment_start = datetime.strptime(segment["@beginning"], "%Y%m%d%H")
                segment_end = datetime.strptime(segment["@ending"], "%Y%m%d%H")
                time_idx_range = date2index([segment_start, segment_end], time_variable)
                pass
        print(self._data_info)