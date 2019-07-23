"""Class cvcCalcUpPDFtailnew provides methods for calculation of n-th percentile
of daily maximum temperature values for 5 consecutive days window of the 30-years Base period.
"""

import datetime
import calendar
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
        for level in vertical_levels:
            start_date = datetime.datetime.strptime(time_segments[0]['@beginning'], '%Y%m%d%H')
            end_date = datetime.datetime.strptime(time_segments[0]['@ending'], '%Y%m%d%H')
            years = [start_date.year + i for i in range(end_date.year - start_date.year + 1)]
            segment_start = datetime.datetime(1, start_date.month, start_date.day, start_date.hour)
            segment_end = datetime.datetime(1, end_date.month, end_date.day, end_date.hour)
            dates_delta = segment_end - segment_start + datetime.timedelta(days=1)  # Days in the segment.
            days = [segment_start + datetime.timedelta(days=i) for i in range(dates_delta.days)]  # Days of the segment.

            for day in days:
                time_segments = []
                for year in years:
                    day_i = datetime.datetime(year, day.month, day.day, day.hour)
                    five_days = {}  # Five-day segment to read.
                    five_days['@beginning'] = (day_i-datetime.timedelta(days=2)).strftime('%Y%m%d%H')
                    five_days['@ending'] = (day0+datetime.timedelta(days=2, hours=23)).strftime('%Y%m%d%H')
                    five_days['@name'] = 'Segment for year {}'.format(year)
                    time_segments.append(five_days)
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
