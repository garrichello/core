"""Class cvcCalcUpPDFtailnew provides methods for calculation of n-th percentile
of daily maximum temperature values for 5 consecutive days window of the 30-years Base period.
"""

import numpy as np

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1
THRESHOLD_UID = 'percentileThreshold'
DEFAULT_THRESHOLD = 90

class cvcCalcUpPDFtailnew(Calc):
    """ Performs calculation of n-th percentile of daily maximum temperatures.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(cvcCalcUpPDFtailnew::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(cvcCalcUpPDFtailnew::run) No input arguments!'

        # Get parameters
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
            if parameters.get(THRESHOLD_UID) is None:  # If the threshold is not set...
                parameters[THRESHOLD_UID] = DEFAULT_THRESHOLD  # give it a default value.
        else:
            parameters = {}
            parameters[THRESHOLD_UID] = DEFAULT_THRESHOLD

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(cvcCalcUpPDFtailnew::run) No output arguments!'

        # Get time segments and levels
        time_segments = self._data_helper.get_segments(input_uids[0])
        vertical_levels = self._data_helper.get_levels(input_uids[0])

        # Get data
        import datetime

        start_date = datetime.datetime(1961, 1, 1, 0, 0)  # Start of the segment.
        end_date = datetime.datetime(1961, 12, 31, 23, 59)  # End of the segment.
        dates_delta = end_date - start_date + datetime.timedelta(days=1)  # Days in the segment.
        list_date = [start_date + datetime.timedelta(days=i) for i in range(dates_delta.days)]  # Days of the segment.
        try:
            feb29 = datetime.datetime(start_date.year, 2, 29)  # Try to create a Feb 29 day.
        except ValueError:
            feb29 = None  # If current year is NOT a leap year.
        if feb29 is not None:
            _ = list_date.remove(feb29)  # If current year IS a leap year.
        five_days = {}  # Five-day segment to read.
        five_days['@beginning'] = (list_date[0]-datetime.timedelta(days=2)).strftime('%Y%m%d%H')
        five_days['@ending'] = (list_date[0]+datetime.timedelta(days=2, hours=23)).strftime('%Y%m%d%H')
        five_days['@name'] = '5-day segment'
        result = self._data_helper.get(input_uids[0], segments=five_days, levels=vertical_levels)

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
                                  fill_value=result['@fill_value'], meta=result['meta'])

        print('(cvcCalcUpPDFtailnew::run) Finished!')
