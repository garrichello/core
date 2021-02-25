""" CalcDTR implements calculation of a spatial field of average daily temperature range.

    Input arguments:
        input_uids[0] -- maximum daily values
        input_uids[1] -- minimum daily values
        input_uids[2] -- module parameters:
            Mode -- string, allowed values:
                'segment' -- for each segment
                'data' -- maximum over all segments
    Output arguments:
        output_uids[0] -- average daily temperature range, data array of size:
            [segments, lats, lons] -- for Mode == 'segment'
            [lats, lons] -- for Mode == 'data'
"""

from copy import deepcopy
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 3
INPUT_PARAMETERS_INDEX = 2
MAX_DATA_UID = 0
MIN_DATA_UID = 1
DEFAULT_VALUES = {'Mode': 'data'}

class CalcDTR(Calc):
    """ Provides calculation of a spatial field of cold/warm nights/days values for time series of data.

    """

    def __init__(self, data_helper: DataAccess):
        super().__init__()
        self._data_helper = data_helper

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        self.logger.info('Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, 'Error! No input arguments!'

        # Get parameters
        parameters = None
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:  # If parameters are given.
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
        calc_mode = self._get_parameter('Mode', parameters, DEFAULT_VALUES)

        self.logger.info('Calculation mode: %s', calc_mode)

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcDTR::run) No output arguments!'

        # Get time segments and levels (only for maximum data, for minimum they must be the same)
        time_segments = self._data_helper.get_segments(input_uids[MAX_DATA_UID])
        vertical_levels = self._data_helper.get_levels(input_uids[MAX_DATA_UID])

        # Set result units.
        input_description = self._data_helper.get_data_info(input_uids[MAX_DATA_UID])['description']
        result_description = {'@units': input_description['@units']}

        data_func = ma.mean  # For calc_mode == 'data' we calculate mean over all segments.

        for level in vertical_levels:
            all_segments_data = []
            for segment in time_segments:
                # Read data
                max_data = self._data_helper.get(input_uids[MAX_DATA_UID], segments=segment, levels=level)
                max_values = max_data['data'][level][segment['@name']]['@values']
                min_data = self._data_helper.get(input_uids[MIN_DATA_UID], segments=segment, levels=level)
                min_values = min_data['data'][level][segment['@name']]['@values']

                # Calculate the difference.
                difference = max_values - min_values

                # Perform calculation for the current time segment.
                one_segment_data = ma.mean(difference, axis=0)

                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if calc_mode == 'segment':
                    self._data_helper.put(output_uids[0], values=one_segment_data, level=level, segment=segment,
                                          longitudes=max_data['@longitude_grid'],
                                          latitudes=max_data['@latitude_grid'],
                                          fill_value=max_data['@fill_value'],
                                          meta=max_data['meta'], description=result_description)
                elif calc_mode == 'data':
                    all_segments_data.append(one_segment_data)
                else:
                    self.logger.error('Error! Unknown calculation mode: \'%s\'', calc_mode)
                    raise ValueError

            # For data-wise analysis analyse segments analyses :)
            if calc_mode == 'data':
                data_out = data_func(ma.stack(all_segments_data), axis=0)

                # Make a global segment covering all input time segments
                full_range_segment = deepcopy(time_segments[0])  # Take the beginning of the first segment...
                full_range_segment['@ending'] = time_segments[-1]['@ending']  # and the end of the last one.
                full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

                self._data_helper.put(output_uids[0], values=data_out, level=level, segment=full_range_segment,
                                      longitudes=max_data['@longitude_grid'], latitudes=max_data['@latitude_grid'],
                                      fill_value=max_data['@fill_value'], meta=max_data['meta'],
                                      description=result_description)

        self.logger.info('Finished!')
