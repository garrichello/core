"""Class cvcCalcTrendTM provides methods for trend calculation"""

import numpy as np

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 1

class cvcCalcTrendTM(Calc):
    """ Performs calculation of trend of values.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(cvcCalcTrendTM::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(cvcCalcTrendTM::run) No input arguments!'

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(cvcCalcTrendTM::run) No output arguments!'

        # Get time segments
        time_segments = self._data_helper.get_segments(input_uids[0])
        if len(time_segments) < 2:
            print("(cvcCalcTrendTM::run) Error! Don't know how to calculate trend: only one input time segment is given. Aborting...")
            raise AssertionError('Input data contains only one time segment!')

        # Get vertical levels
        vertical_levels = self._data_helper.get_levels(input_uids[0])

        # Get data for all time segments and levels
        result = self._data_helper.get(input_uids[0], segments=time_segments, levels=vertical_levels)

        data_info = self._data_helper.get_data_info(input_uids[0])
        description = data_info['description']
        description['@title'] = 'Trend of ' + description['@title']
        description['@name'] = 'Trend of ' + description['@name']
        description['@units'] += '/10yr'

        for level in vertical_levels:
            sum_y = 0
            cnt = 0
            for segment in time_segments:
                sum_y += result['data'][level][segment['@name']]['@values'].filled(0)  # Sum values
                cnt += (~result['data'][level][segment['@name']]['@values'].mask).astype(int)  # Count valid values
            mean_y = np.ma.MaskedArray(sum_y, mask=~cnt.astype(bool))  # Count values are inverted to create a mask
            mean_y /= cnt  # Calculate mean value only for valid values
            mean_x = np.mean(range(len(time_segments)))  # Just a simple mean of a simple x-axis

            num_arr = 0
            den_arr = 0
            x = 0
            for segment in time_segments:
                num_arr += (result['data'][level][segment['@name']]['@values'] - mean_y) * (x - mean_x)
                den_arr += (x - mean_x)**2
                x += 1
            trend_values = (num_arr / den_arr) * 10.0

            global_segment = self.make_global_segment(time_segments)
            self._data_helper.put(output_uids[0], values=trend_values, level=level, segment=global_segment,
                                  longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                  description=description,
                                  fill_value=result['@fill_value'], meta=result['meta'])

        print('(cvcCalcTrendTM::run) Finished!')
