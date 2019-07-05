"""Class for output"""

from core.base.dataaccess import DataAccess

from core.base.common import print, kelvin_to_celsius, celsius_to_kelvin

MINIMUM_POSSIBLE_TEMPERATURE_K = celsius_to_kelvin(-90.0)  # -89.2 degC is the minimum registered temperature on Earth
MAXIMUM_POSSIBLE_TEMPERATURE_K = celsius_to_kelvin(60.0)  # 56.7 degC is the maximum registered temperature on Earth

class cvcOutput:
    """ Provides redirection of input data arrays to corresponding plotting/writing modules

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def run(self):
        """ Passes output data array to a write method of a data module (through data access helper0) """

        print('(cvcOutput::run) Started!')

        input_uids = self._data_helper.input_uids()

        time_segments = self._data_helper.get_segments(input_uids[0])

        vertical_levels = self._data_helper.get_levels(input_uids[0])

        # Get data for all time segments and levels at once
        result = self._data_helper.get(input_uids[0], segments=time_segments, levels=vertical_levels)

        output_uids = self._data_helper.output_uids()

        description = result['data']['description']

        # Check if data are in K and we need to convert them to C.
        CONVERT_K2C = False
        if ('@tempk2c' in description) & ('@units' in description):
            if (description['@tempk2c'] == 'yes') & (description['@units'] == 'K'):
#                & (values.min() > MINIMUM_POSSIBLE_TEMPERATURE_K) \
#                & (values.max() < MAXIMUM_POSSIBLE_TEMPERATURE_K):
                description['@units'] = 'C'
                CONVERT_K2C = True

        for level_name in vertical_levels:
            for segment in time_segments:
                values = result['data'][level_name][segment['@name']]['@values']

                # Convert Kelvin to Celsius if asked and appropriate
                if CONVERT_K2C:
                    values = kelvin_to_celsius(values)

                self._data_helper.put(output_uids[0], values, level=level_name, segment=segment,
                                      longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                      description=description, meta=result['meta'])

        print('(cvcOutput::run) Finished!')
