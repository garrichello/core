"""Provides classes
    DataHdfeos
"""

from datetime import datetime

import re
import numpy as np
import numpy.ma as ma

from base.common import listify, unlistify, print, make_filename
from mod.data.data import Data

NO_LEVEL_NAME = 'none'

class DataHdfeos(Data):
    """ Provides methods for reading and writing archives of HDF4 files.
    """
    def __init__(self, data_info):
        self._data_info = data_info
        super().__init__(data_info)

    def read(self, options):
        """Reads HDF-EOS file into an array.

        Arguments:
            options -- dictionary of read options:
                ['segments'] -- time segments
                ['levels'] -- vertical levels

        Returns:
            result['array'] -- data array
        """

        print('(DataHdfeos::read) Reading HDF-EOS data...')

        pass


    def write(self, values, options):
        """Writes data array into a HDF-EOS file.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options:
                ['level'] -- vertical level name 
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
        """    
        
        print('(DataHdfeos::write) Writing data to a HDF-EOS file...')

        pass
