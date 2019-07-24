"""Class cvcCalcUpPDFtailnew provides methods for calculation of 10th and 90th percentile
of daily maximum temperature values for 5 consecutive days window of the 30-year Base period.
"""

import datetime
import numpy as np

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
THRESHOLDS = [10, 90]

class cvcCalcUpPDFtailnew(Calc):
    """ Performs calculation of n-th percentile of daily maximum temperatures.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def _calc_percentile(self, uid, threshold):
        """ Calculates given percentile at given vertical levels for a given time period.
        Arguments:
            uid -- UID of input dataset.
            threshold -- percentile to calculate.
        Returns:
            percentile -- 2-D masked array of percentiles.
        """

        # Get time segments and levels
        time_segment = self._data_helper.get_segments(uid)[0]  # Only the first time segment is taken.
        vertical_levels = self._data_helper.get_levels(uid)

        all_percentiles = {}
        all_percentiles['data'] = {}
        for level in vertical_levels:
            start_date = datetime.datetime.strptime(time_segment['@beginning'], '%Y%m%d%H')
            end_date = datetime.datetime.strptime(time_segment['@ending'], '%Y%m%d%H')
            years = [start_date.year + i for i in range(end_date.year - start_date.year + 1)]
            segment_start = datetime.datetime(1, start_date.month, start_date.day, start_date.hour)
            segment_end = datetime.datetime(1, end_date.month, end_date.day, end_date.hour)
            dates_delta = segment_end - segment_start + datetime.timedelta(days=1)  # Days in the segment.
            days = [segment_start + datetime.timedelta(days=i) for i in range(dates_delta.days)]  # Days of the segment.

            # For each day of the year (segment) take 5-day window centered in this day for all years (30).
            # Concatenate 5 days x 30 years to obtain 150 lon-lat grids.
            # Calculate percentile along time axis for each cell of the grid to get 2-D grid of percentiles.
            # Make a Masked array on the base of the percentile grid using mask from one of the 5-day arrays.
            # We suppose that the mask is the same for all lon-lat grids along the time axis.
            percentile = []
            for day in days:
                segments = []
                for year in years:
                    day_i = datetime.datetime(year, day.month, day.day, day.hour)
                    five_days = {}  # Five-day segment to read.
                    five_days['@beginning'] = (day_i - datetime.timedelta(days=2)).strftime('%Y%m%d%H')
                    five_days['@ending'] = (day_i + datetime.timedelta(days=2, hours=23)).strftime('%Y%m%d%H')
                    five_days['@name'] = 'Year {}'.format(year)
                    segments.append(five_days)
                result = self._data_helper.get(uid, segments=segments, levels=vertical_levels)
                data = np.ma.concatenate(
                    [result['data'][level]['Year {}'.format(year)]['@values'] for year in years], axis=0)
                percentile.append(np.percentile(data, threshold, axis=0))
            percentile = np.stack(percentile, axis=0)  # Stack to array.
            mask_0 = result['data'][level]['Year {}'.format(years[0])]['@values'].mask[0]  # lon-lat mask.
            mask_shape = [1] * (mask_0.ndim + 1)  # New shape of the mask.
            mask_shape[0] = len(days)  # Set it to be (n_days, 1, 1) or (n_days, 1).
            mask = np.tile(mask_0, mask_shape)  # Generate a mask to conform data dimensions.
            all_percentiles['data'][level] = np.ma.MaskedArray(percentile, mask=mask, fill_value=result['@fill_value'])

        all_percentiles['@base_period'] = time_segment
        all_percentiles['@day_grid'] = days
        all_percentiles['@longitude_grid'] = result['@longitude_grid']
        all_percentiles['@latitude_grid'] = result['latitude_grid']
        all_percentiles['@fill_value'] = result['@fill_value']
        all_percentiles['meta'] = result['meta']

        return all_percentiles

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(cvcCalcUpPDFtailnew::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(cvcCalcUpPDFtailnew::run) No input arguments!'

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(cvcCalcUpPDFtailnew::run) No output arguments!'

        # Calculate percentiles
        input_varname = ''
        out_uid = 0
        for in_uid in input_uids:
            for threshold in THRESHOLDS:
                percentile = self._calc_percentile(in_uid, threshold)
                percentile['meta']['varname'] = '{}{}p'.format(input_varname, threshold)
                for level, data in percentile['data']:
                    self._data_helper.put(output_uids[out_uid], values=data, level=level, segment=percentile['@base_period'],
                                        longitudes=percentile['@longitude_grid'], latitudes=percentile['@latitude_grid'],
                                        times=percentile['@day_grid'], fill_value=percentile['@fill_value'], meta=percentile['meta'])
                out_uid += 1

        print('(cvcCalcUpPDFtailnew::run) Finished!')
