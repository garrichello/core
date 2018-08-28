"""Provides classes
    DataArray
"""
from base.common import listify, print

class DataArray:
    """ Provides methods for reading and writing arrays from/to memory.
    """

    def __init__(self, data_info):
        self._data_info = data_info

        # Create a new levels element in the data info discarding anything specified in the task file.
        self._data_info['data']['levels'] = {}
        self._data_info['data']['levels']['@values'] = set()

        # Create a new time segment element in data info discarding anything specified in the task file.
        self._data_info['data']['time'] = {}
        self._data_info['data']['time']['segment'] = []

    def read(self, options):
        """Reads an array.

        Arguments:
            options -- dictionary of read options:
                ['segments'] -- time segments
                ['levels'] -- vertical levels

        Returns:
            result['array'] -- data array
        """

        # Levels must be a list or None.
        levels_to_read = listify(options['levels'])
        if levels_to_read is None:
            levels_to_read = self._data_info['levels']  # Read all levels if nothing specified.
        # Segments must be a list or None.
        segments_to_read = listify(options['segments'])
        if segments_to_read is None:
            segments_to_read = listify(self._data_info['data']['time']['segment'])  # Read all levels if nothing specified.
        
        result = {} # Contains data arrays, grids and some additional information.
        result['data'] = {} # Contains data arrays being read from netCDF files at each vertical level.

        # Process each vertical level separately.
        for level_name in levels_to_read:
            print ('(DataArray::read) Reading level: \'{0}\''.format(level_name))

            # Process each time segment separately.
            data_by_segment = {} # Contains data array for each time segment.
            for segment in segments_to_read:
                print ('(DataArray::read) Reading time segment \'{0}\''.format(segment['@name']))

                data_by_segment[segment['@name']] = {}
                data_by_segment[segment['@name']]['@values'] = self._data_info['data'][level_name][segment['@name']]['@values']
                data_by_segment[segment['@name']]['description'] = self._data_info['data']['description']
                data_by_segment[segment['@name']]['@time_grid'] = self._data_info['data']['time'].get('@grid')
                data_by_segment[segment['@name']]['segment'] = segment
            
            result['data'][level_name] = data_by_segment
            result['@longitude_grid'] = self._data_info['data']['@longitudes']
            result['@latitude_grid'] = self._data_info['data']['@latitudes']
            result['@fill_value'] = self._data_info['data'][level_name][segment['@name']]['@values'].fill_value
            result['meta'] = self._data_info['meta']
        
        return result

    def write(self, values, options):
        """ Stores values and metadata in data_info dictionary
            describing 'array' data element.
                
        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options:
                ['level'] -- vertical level name 
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
        """ 

        level = options['level']
        segment = options['segment']
        times = options['times']
        longitudes = options['longitudes']
        latitudes = options['latitudes']
        meta = options['meta']

        if level is not None:
            # Append a new level name.
            self._data_info['data']['levels']['@values'].add(level)

        if segment is not None:
            # Append a new time segment.
            try:
                self._data_info['data']['time']['segment'].index(segment)
            except ValueError:
                self._data_info['data']['time']['segment'].append(segment)

        if times is not None:
            # Append a time grid
            self._data_info['data']['time']['@grid'] = times

        self._data_info['data']['@longitudes'] = longitudes
        self._data_info['data']['@latitudes'] = latitudes

        if level is not None:
            if self._data_info['data'].get(level) is None:
                self._data_info['data'][level] = {}
            if segment is not None:
                if self._data_info['data'][level].get(segment['@name']) is None:
                    self._data_info['data'][level][segment['@name']] = {}
                self._data_info['data'][level][segment['@name']]['@values'] = values
            else:
                self._data_info['data'][level]['@values'] = values
        else:
           self._data_info['data']['@values'] = values

        self._data_info['meta'] = meta