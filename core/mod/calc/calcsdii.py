"""Class CalcSDII provides methods for calculation of simple precipitation intensity index (SDII).

    Input arguments:
        input_uids[0] -- total precipitation values
        input_uids[1] -- module parameters:
            Mode -- string, allowed values:
                'segment' -- for each segment
                'data' -- maximum over all segments

    Output arguments:
        output_uids[0] -- simple precipitation intensity index array:
            [segments, lats, lons] -- for Mode == 'segment'
            [lats, lons] -- for Mode == 'data'

"""

from itertools import groupby
from copy import deepcopy
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1
DATA_UID = 0
DEFAULT_VALUES = {'Mode': 'data'}

class CalcSDII(Calc):
    """ Performs calculation of simple precipitation intensity index.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def calc_sdii(self, values, time_grid):
        """ Calculates SDII
        Arguments:
            values -- array of total precipitation
            time_grid -- time grid
        Returns: Rxnday values
        """

        threshold = 1  # Wet day threshold (1 mm)
        w_days = None  # Wet days (daily precipitation >= 1 mm) counter.
        rr_sum = None  # Sum of daily precipitation in wet days.
        it_all_data = groupby(zip(values, time_grid), key=lambda x: (x[1].day, x[1].month))
        for _, one_day_group in it_all_data:  # Iterate over daily groups.
            daily_sum = self._calc_daily_sum(one_day_group)
            mask = daily_sum > threshold
            if w_days is None:
                w_days = ma.zeros(daily_sum.shape)
            w_days += mask
            if rr_sum is None:
                rr_sum = ma.zeros(daily_sum.shape)
            rr_sum[mask] += daily_sum[mask]

        result = rr_sum / w_days  # Masked array automatically hides NaNs created by division by zero.

        return result

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(CalcSDII::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(CalcSDII::run) No input arguments!'

        # Get parameters
        parameters = None
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:  # If parameters are given.
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
        calc_mode = self._get_parameter('Mode', parameters, DEFAULT_VALUES)

        print('(CalcSDII::run) Calculation mode: {}'.format(calc_mode))

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcSDII::run) No output arguments!'

        # Get time segments and levels
        time_segments = self._data_helper.get_segments(input_uids[DATA_UID])
        vertical_levels = self._data_helper.get_levels(input_uids[DATA_UID])

        data_func = ma.max  # For calc_mode == 'data' we calculate max over all segments.

        # Main loop
        for level in vertical_levels:
            all_segments_data = []
            for segment in time_segments:
                # Read data
                data = self._data_helper.get(input_uids[DATA_UID], segments=segment, levels=level)
                values = data['data'][level][segment['@name']]['@values']
                time_grid = data['data'][level][segment['@name']]['@time_grid']

                one_segment_data = self.calc_sdii(values, time_grid)

                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if calc_mode == 'segment':
                    self._data_helper.put(output_uids[0], values=one_segment_data, level=level, segment=segment,
                                          longitudes=data['@longitude_grid'],
                                          latitudes=data['@latitude_grid'],
                                          fill_value=data['@fill_value'],
                                          meta=data['meta'])
                elif calc_mode == 'data':
                    all_segments_data.append(one_segment_data)
                else:
                    print('(CalcSDII::run) Error! Unknown calculation mode: \'{}\''.format(calc_mode))
                    raise ValueError

            # For data-wise analysis analyse segments analyses :)
            if calc_mode == 'data':
                data_out = data_func(ma.stack(all_segments_data), axis=0)

                # Make a global segment covering all input time segments
                full_range_segment = deepcopy(time_segments[0])  # Take the beginning of the first segment...
                full_range_segment['@ending'] = time_segments[-1]['@ending']  # and the end of the last one.
                full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

                self._data_helper.put(output_uids[0], values=data_out, level=level, segment=full_range_segment,
                                      longitudes=data['@longitude_grid'], latitudes=data['@latitude_grid'],
                                      fill_value=data['@fill_value'], meta=data['meta'])


        print('(CalcSDII::run) Finished!')
