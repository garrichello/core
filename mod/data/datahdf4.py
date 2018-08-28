"""Provides classes
    DataHdf4
"""

from datetime import datetime

from string import Template
import re
import numpy as np
import numpy.ma as ma
from matplotlib.path import Path

from base.common import listify, unlistify, print, make_filename

LONGITUDE_UNITS =  {'degrees_east', 'degree_east', 'degrees_E', 'degree_E', 'degreesE', 'degreeE', 'lon'}
LATITUDE_UNITS = {'degrees_north', 'degree_north', 'degrees_N', 'degree_N', 'degreesN', 'degreeN', 'lat'}
TIME_UNITS = {'since', 'time'}
NO_LEVEL_NAME = 'none'

class PercentTemplate(Template):
    """ Custom template for the string substitute method.
        It changes the template delimiter to %<template>%
    """
    delimiter = '%'
    pattern = r'''
    \%(?:
        (?P<escaped>%) |
        (?P<named>[_a-z][_a-z0-9\-]*)% |
        \b\B(?P<braced>) |
        (?P<invalid>)
    )
    '''

class DataHdf4:
    """ Provides methods for reading and writing archives of HDF4 files.
    """
    def __init__(self, data_info):
        self._data_info = data_info


    def read(self, options):
        """Reads HDF4-file into an array.

        Arguments:
            options -- dictionary of read options:
                ['segments'] -- time segments
                ['levels'] -- vertical levels

        Returns:
            result['array'] -- data array
        """

        print('(DataHDF4::read) Reading HDF4...')

        pass


    def write(self, values, options):
        """Writes data array into a HDF4 file.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options:
                ['level'] -- vertical level name 
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
        """    
        
        print('(DataHDF4::write) Writing HDF4...')

        pass
