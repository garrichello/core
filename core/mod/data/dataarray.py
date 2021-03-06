"""Provides classes
    DataArray
"""
from core.base.common import listify

from .data import Data


class DataArray(Data):
    """ Provides methods for reading and writing arrays from/to memory.
    """

    def __init__(self, data_info):
        super().__init__(data_info)
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

        self.logger.info('Reading memory data array %s...', self._data_info['data']['@uid'])

        # Levels must be a list or None.
        levels_to_read = listify(options['levels'])
        if levels_to_read is None:
            levels_to_read = self._data_info['levels']  # Read all levels if nothing specified.
        # Segments must be a list or None.
        segments_to_read = listify(options['segments'])
        if segments_to_read is None:
            segments_to_read = listify(self._data_info['data']['time']['segment'])  # Read all levels if nothing specified.

        # Process each vertical level separately.
        level_name = None
        for level_name in levels_to_read:
            self.logger.info('Reading level: \'%s\'', level_name)

            # Process each time segment separately.
            self._init_segment_data(level_name)  # Initialize a data dictionary for the vertical level 'level_name'.
            segment = None
            for segment in segments_to_read:
                self.logger.info('Reading time segment \'%s\'', segment['@name'])
                self.logger.info('Min data value: %s, max data value: %s',
                                 self._data_info['data'][level_name][segment['@name']]['@values'].min(),
                                 self._data_info['data'][level_name][segment['@name']]['@values'].max())
                self._add_segment_data(level_name=level_name,
                                       values=self._data_info['data'][level_name][segment['@name']]['@values'],
                                       time_grid=self._data_info['data'][level_name][segment['@name']]['@time_grid'],
                                       time_segment=segment)
                self.logger.info('Done!')

        self._add_metadata(longitude_grid=self._data_info['data']['@longitudes'],
                           latitude_grid=self._data_info['data']['@latitudes'],
                           fill_value=self._data_info['data'][level_name][segment['@name']]['@values'].fill_value,
                           description=self._data_info['data']['description'], meta=self._data_info['meta'])

        self.logger.info('Done!')

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

        self.logger.info('Creating memory data array...')

        level = options['level'] if options['level'] is not None else 'none'
        segment = options['segment'] if options['segment'] is not None else {'@name': 'none'}
        times = options['times'] if options['times'] is not None else []
        longitudes = options['longitudes']
        latitudes = options['latitudes']
        description = options['description']
        meta = options['meta']

        self._data_info['data']['levels']['@values'].add(level)
        if segment not in self._data_info['data']['time']['segment']:
            self._data_info['data']['time']['segment'].append(segment)
        self._data_info['data']['@longitudes'] = longitudes
        self._data_info['data']['@latitudes'] = latitudes

        if level not in self._data_info['data']:
            self._data_info['data'][level] = {}
        if segment['@name'] not in self._data_info['data'][level]:
            self._data_info['data'][level][segment['@name']] = {}
        self._data_info['data'][level][segment['@name']]['@time_grid'] = times
        self._data_info['data'][level][segment['@name']]['@values'] = values

        if 'description' not in self._data_info['data'].keys():
            self._data_info['data']['description'] = {}
        if description is not None:
            self._data_info['data']['description'].update(description)
        if 'meta' not in self._data_info.keys():
            self._data_info['meta'] = {}
        if meta is not None:
            self._data_info['meta'].update(meta)

        self.logger.info('Done!')
