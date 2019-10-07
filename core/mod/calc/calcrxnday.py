"""Class CalcRxnday provides methods for calculation of monthly maximum consecutive n-day precipitation.

    Input arguments:
        input_uids[0] -- total precipitation values
        input_uids[1] -- module parameters:
            Mode -- string, allowed values:
                'segment' -- for each segment
                'data' -- maximum over all segments
            Threshold -- integer, n - number of consecutive days.

    Output arguments:
        output_uids[0] -- monthly maximum consecutive n-day precipitation array:
            [segments, lats, lons] -- for Mode == 'segment'
            [lats, lons] -- for Mode == 'data'

"""

from collections import deque
from itertools import groupby
from copy import deepcopy
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1
DATA_UID = 0
DEFAULT_VALUES = {'Mode': 'data', 'Threshold': 5}

class CalcRxnday(Calc):
    """ Performs calculation of  monthly maximum consecutive n-day precipitation.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def calc_rxndays(self, values, time_grid, threshold):
        """ Calculates Rxnday
        Arguments:
            values -- array of total precipitation
            time_grid -- time grid
            threshold -- number of days (n)
        Returns: Rxnday values
        """

        # First of all, we suppose values are for a month with some time step.
        # Initially, let's sum first 'threshold' days.
        # Then we slide a window along the time grid
        #  subtracting one day on the left and adding one day on the right.
        queue = deque()
        nday_sum = None
        max_sum = None
        it_all_data = groupby(zip(values, time_grid), key=lambda x: (x[1].day, x[1].month))
        for _, one_day_group in it_all_data:  # Iterate over daily groups.
            daily_sum = None
            for one_step_data, _ in one_day_group:  # Iterate over each time step in each group.
                if daily_sum is None:
                    daily_sum = deepcopy(one_step_data)
                else:
                    daily_sum += one_step_data  # Calculate daily sums.
            queue.append(daily_sum)  # Store daily sums in a queue.
            if nday_sum is None:
                nday_sum = deepcopy(daily_sum)
            else:
                nday_sum += daily_sum  # Additionally sum daily sums.
            if len(queue) > threshold:  # When 'threshold' days are summed...
                nday_sum -= queue.popleft()  # ...subtruct one 'left-most' daily sum from the n-day sum.
            if len(queue) == threshold:
                if max_sum is None:
                    max_sum = deepcopy(nday_sum)
                else:
                    max_sum = ma.max(ma.stack((max_sum, nday_sum)), axis=0)  # Search for maximum value.

        return max_sum

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(CalcRxnday::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(CalcRxnday::run) No input arguments!'

        # Get parameters
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:  # If parameters are given.
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
            threshold = self._get_parameter('Threshold', parameters, DEFAULT_VALUES)
            calc_mode = self._get_parameter('Mode', parameters, DEFAULT_VALUES)

        print('(CalcRxnday::run) Threshold: {}'.format(threshold))
        print('(CalcRxnday::run) Calculation mode: {}'.format(calc_mode))

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcRxnday::run) No output arguments!'

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

                one_segment_data = self.calc_rxndays(values, time_grid, threshold)

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
                    print('(CalcRxnday::run) Error! Unknown calculation mode: \'{}\''.format(calc_mode))
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


        print('(CalcRxnday::run) Finished!')
