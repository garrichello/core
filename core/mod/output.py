"""Class for output"""

import logging
from core.base.dataaccess import DataAccess

from core.base.common import kelvin_to_celsius, celsius_to_kelvin

MINIMUM_POSSIBLE_TEMPERATURE_K = celsius_to_kelvin(-90.0)  # -89.2 degC is the minimum registered temperature on Earth
MAXIMUM_POSSIBLE_TEMPERATURE_K = celsius_to_kelvin(60.0)  # 56.7 degC is the maximum registered temperature on Earth

class cvcOutput:
    """ Provides redirection of input data arrays to corresponding plotting/writing modules

    """

    def __init__(self, data_helper: DataAccess):
        self.logger = logging.getLogger()
        self._data_helper = data_helper

    def run(self):
        """ Passes output data array to a write method of a data module (through data access helper0) """

        self.logger.info('Started!')

        input_uids = self._data_helper.input_uids()

        output_uids = self._data_helper.output_uids()
        if output_uids is None:
            self.logger.error("""No output is specified! Check the task file, may be you are using the old template?
                                 Destination description should be passed as _output_ argument to process cvcOutput.""")
            raise ValueError('No output dataset specified. Aborting!')

        output_info = self._data_helper.get_data_info(output_uids[0])
        # For image and raw output we'll pass everything at once. So collect'em all here!
        if output_info['@type'] == 'image' or output_info['@type'] == 'raw':
            all_values = []
            all_times = []
            all_description = []
            all_meta = []

        for in_uid in input_uids:
            time_segments = self._data_helper.get_segments(in_uid)
            vertical_levels = self._data_helper.get_levels(in_uid)

            # Get data for all time segments and levels at once
            result = self._data_helper.get(in_uid, segments=time_segments, levels=vertical_levels)
            if result['data']['@type'] == 'parameter':
                continue
            description = result['data']['description']
            all_description.append(description)

            # Check if data are in K and we need to convert them to C.
            convert_k2c = False
            if ('@tempk2c' in description) & ('@units' in description):
                if (description['@tempk2c'] == 'yes') & (description['@units'] == 'K'):
    #                & (values.min() > MINIMUM_POSSIBLE_TEMPERATURE_K) \
    #                & (values.max() < MAXIMUM_POSSIBLE_TEMPERATURE_K):
                    description['@units'] = 'C'
                    convert_k2c = True

            for level_name in vertical_levels:
                for segment in time_segments:
                    values = result['data'][level_name][segment['@name']]['@values']

                    # Convert Kelvin to Celsius if asked and appropriate
                    if convert_k2c:
                        values = kelvin_to_celsius(values)

                    # Collect all data and meta.
                    if output_info['@type'] == 'image' or output_info['@type'] == 'raw':
                        all_values.append(values)
                        all_times.append(result['data'][level_name][segment['@name']]['@time_grid'])
                        all_meta.append(result['meta'])
                    else:  # Pass one by one to a writer.
                        self._data_helper.put(output_uids[0], values, level=level_name, segment=segment,
                                              longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                              times=result['data'][level_name][segment['@name']]['@time_grid'],
                                              description=description, meta=result['meta'])

            # Pass everything in one uid at once to raw output.
            if output_info['@type'] == 'raw':
                self._data_helper.put(output_uids[0], all_values, level=vertical_levels, segment=time_segments,
                                      longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                      times=all_times, description=description, meta=result['meta'])
                all_values = []
                all_times = []

        # Pass everything in all uids at once to image output.
        if output_info['@type'] == 'image':
            self._data_helper.put(output_uids[0], all_values, level=vertical_levels, segment=time_segments,
                                  longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                  times=all_times, description=all_description, meta=all_meta)

        self.logger.info('Finished!')
