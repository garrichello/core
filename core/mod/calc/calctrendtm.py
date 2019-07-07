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

        # Get data for all time segments and levels at once
        time_segments = self._data_helper.get_segments(input_uids[0])
        vertical_levels = self._data_helper.get_levels(input_uids[0])
        result = self._data_helper.get(input_uids[0], segments=time_segments, levels=vertical_levels)

        for level in vertical_levels:
            sum_y = 0
            cnt = 0
            for segment in time_segments:
                sum_y += result['data'][level][segment['@name']]['values'].filled(0)  # Sum values
                cnt += (~result['data'][level][segment['@name']]['values'].mask).astype(int)  # Count valid values
            mean_y = np.ma.MaskedArray(sum_y, mask=~cnt.astype(bool))  # Count values are inverted to create a mask
            mean_y /= cnt  # Calculate mean value only for valid values

            trend_values = None
            global_segment = self.make_global_segment(time_segments)
            self._data_helper.put(output_uids[0], values=trend_values, level=level, segment=global_segment,
                                      longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                      fill_value=result['@fill_value'], meta=result['meta'])

        print('(cvcCalcTrendTM::run) Finished!')
