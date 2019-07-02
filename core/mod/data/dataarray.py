"""Provides classes
    DataArray
"""
from ...base.common import listify, print

from .data import Data


class DataArray(Data):
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

        super().__init__(data_info)

    def read(self, options):
        """Reads an array.

        Arguments:
            options -- dictionary of read options:
                ['segments'] -- time segments
                ['levels'] -- vertical levels

        Returns:
            result['array'] -- data array
        """

        print('(DataArray::read) Reading memory data array {}...'.format(self._data_info['data']['@uid']))

        # Levels must be a list or None.
        levels_to_read = listify(options['levels'])
        if levels_to_read is None:
            levels_to_read = self._data_info['levels']  # Read all levels if nothing specified.
        # Segments must be a list or None.
        segments_to_read = listify(options['segments'])
        if segments_to_read is None:
            segments_to_read = listify(self._data_info['data']['time']['segment'])  # Read all levels if nothing specified.

        result = {}  # Contains data arrays, grids and some additional information.
        result['data'] = {}  # Contains data arrays being read from netCDF files at each vertical level.

        # Process each vertical level separately.
        level_name = None
        for level_name in levels_to_read:
            print('(DataArray::read)  Reading level: \'{0}\''.format(level_name))

            # Process each time segment separately.
            self._init_segment_data(level_name)  # Initialize a data dictionary for the vertical level 'level_name'.
            segment = None
            for segment in segments_to_read:
                print('(DataArray::read)  Reading time segment \'{0}\''.format(segment['@name']))
                print('(DataArray::read)   Min data value: {}, max data value: {}'.format(
                    self._data_info['data'][level_name][segment['@name']]['@values'].min(),
                    self._data_info['data'][level_name][segment['@name']]['@values'].max()))
                self._add_segment_data(level_name=level_name,
                                       values=self._data_info['data'][level_name][segment['@name']]['@values'],
                                       description=self._data_info['data']['description'],
                                       time_grid=self._data_info['data']['time'].get('@grid'),
                                       time_segment=segment)
                print('(DataArray::read)  Done!')

        self._add_metadata(longitude_grid=self._data_info['data']['@longitudes'],
                           latitude_grid=self._data_info['data']['@latitudes'],
                           fill_value=self._data_info['data'][level_name][segment['@name']]['@values'].fill_value,
                           meta=self._data_info['meta'])

        print('(DataArray::read) Done!')

        return self._get_result_data()

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
                description -- dictionary describing data:
                    ['title'] -- general title of the data (e.g., Average)
                    ['name'] --  name of the data (e.g., Temperature)
                    ['units'] -- units of th data (e.g., K)
                meta -- additional metadata passed from data readers to data writers through data processors
        """

        print('(DataArray:write) Creating memory data array...')

        level = options['level']
        segment = options['segment']
        times = options['times']
        longitudes = options['longitudes']
        latitudes = options['latitudes']
        description = options['description']
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

        if description is not None:
            self._data_info['data']['description'] = description
        self._data_info['meta'] = meta

        print('(DataArray::write) Done!')
