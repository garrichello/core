"""Provides classes
    DataDB
"""
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from datetime import datetime

from base.common import listify, unlistify, print

class DataDb:
    """ Provides methods for reading and writing geodatabase files.
    """
    def __init__(self, data_info):
        self._data_info = data_info


    def read(self, options):
        """Queries database for in-situ measurements and puts them into an array.

        Arguments:
            options -- dictionary of read options:
                ['segments'] -- time segments
                ['levels'] -- vertical levels

        Returns:
            result['array'] -- data array
        """

        # Segments must be a list or None.
        self._segments = listify(options['segments'])
        self._levels = listify(options['levels'])

        result = {} # Contains data arrays, grids and some additional information.
        result['data'] = {} # Contains data arrays being read from netCDF files at each vertical level.

        # Process each vertical level separately.
        for level_name in self._data_info["levels"]:
            print ('(DataDb::read) Reading level: \'{0}\''.format(level_name))
        
        
        return result

    def write(self, values, options):
        """Writes data array into a database tables.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options:
                ['level'] -- vertical level name 
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
        """    
        
        print('(DataDB::write) Writing DB...')
        print('(DataDB::write) Not implemented yet!')
        pass