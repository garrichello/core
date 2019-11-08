"""Class CalcPercentile provides methods for calculation of n-th percentiles of daily values for the 30-year Base period.
"""

import datetime
import numpy as np

from core.base.dataaccess import DataAccess
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1
DATA_UID = 0
DEFAULT_VALUES = {'Condition': None, 'Threshold': 95}

class CalcPercentile(Calc):
    """ Performs calculation of n-th percentile of given values.

    """

    def __init__(self, data_helper: DataAccess):
        super().__init__()
        self._data_helper = data_helper

    def _calc_percentile(self, uid, n, level, condition=None):
        """ Calculates n-th percentile at a given vertical level for a given time period.
        Arguments:
            uid -- UID of input dataset,
            n -- percentile to calculate,
            level -- vertical level name,
            condition -- control word for applying of special condition to input data:
                'wet' -- data must be greater or equal to 1 mm (applicable to precipitation only)
                None -- no special condition.
        Returns:
            normals -- [time, lat, lon] masked array of n-th perceniles.
        """

        # Get time segments and levels and data info.
        time_segment = self._data_helper.get_segments(uid)[0]  # Only the first time segment is taken.
        data_info = self._data_helper.get_data_info(uid)

        start_date = datetime.datetime.strptime(time_segment['@beginning'], '%Y%m%d%H')
        end_date = datetime.datetime.strptime(time_segment['@ending'], '%Y%m%d%H')
        years = [start_date.year + i for i in range(end_date.year - start_date.year + 1)]
        segment_start = datetime.datetime(1, start_date.month, start_date.day, start_date.hour)
        segment_end = datetime.datetime(1, end_date.month, end_date.day, end_date.hour)
        dates_delta = segment_end - segment_start + datetime.timedelta(days=1)  # Days in the segment.
        days = [segment_start + datetime.timedelta(days=i) for i in range(dates_delta.days)]  # Days of the segment.

        # For each day of the year (segment) this day for all years (30).
        # Concatenate for 30 years to obtain 30 lon-lat grids.
        # Calculate normals along time axis for each cell of the grid to get 2-D grid.
        # Make a Masked array using mask from one of the 1-day arrays.
        # We suppose that the mask is the same for all lon-lat grids along the time axis.
        # Concatenate 2-D grids for all days along time axis to get [time, lat, lon] result array.
        percentile = {}
        all_days_data = []
        for day in days:
            segments = []
            for year in years:
                day_i = datetime.datetime(year, day.month, day.day, day.hour, day.minute)
                one_day = {}  # 1-day segment to read.
                one_day['@beginning'] = day_i.strftime('%Y%m%d%H')
                one_day['@ending'] = day_i.strftime('%Y%m%d%H')
                one_day['@name'] = 'Year {}'.format(year)
                segments.append(one_day)
            result = self._data_helper.get(uid, segments=segments, levels=level)
            data = np.ma.stack(
                [result['data'][level]['Year {}'.format(year)]['@values'] for year in years])
            if condition is not None:
                self._apply_condition(data, condition)
            all_days_data.append(np.percentile(data, n, axis=0))

        percentile_data = np.stack(all_days_data)  # Stack to array.
        mask_0 = result['data'][level]['Year {}'.format(years[0])]['@values'].mask  # lon-lat mask.
        mask_shape = [1] * (mask_0.ndim + 1)  # New shape of the mask.
        mask_shape[0] = len(days)  # Set it to be (n_days, 1, 1) or (n_days, 1).
        mask = np.tile(mask_0, mask_shape)  # Generate a mask to conform data dimensions.
        percentile['data'] = np.ma.MaskedArray(percentile_data, mask=mask, fill_value=result['@fill_value'])

        percentile['@base_period'] = time_segment
        percentile['@day_grid'] = days
        percentile['@longitude_grid'] = result['@longitude_grid']
        percentile['@latitude_grid'] = result['@latitude_grid']
        percentile['@fill_value'] = result['@fill_value']
        percentile['meta'] = result['meta']
        percentile['meta']['varname'] = data_info['variable']['@name'] + '_percentiles'
        percentile['meta']['time_long_name'] = 'Calendar day of the year'
        percentile['meta']['level_units'] = 'percentile'
        percentile['meta']['level_long_name'] = 'Percentile'

        return percentile

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        self.logger.info('Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, 'Error! No input arguments!'

        level = self._data_helper.get_levels(input_uids[DATA_UID])[0]  # Only the first vertical level is taken.

        # Get parameters
        parameters = None
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:  # If parameters are given.
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
        threshold = self._get_parameter('Threshold', parameters, DEFAULT_VALUES)
        condition = self._get_parameter('Condition', parameters, DEFAULT_VALUES)

        self.logger.info('Threshold: %s', threshold)
        self.logger.info('Condition: %s', condition)

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, 'Error! No output arguments!'

        # Calculate percentile
        percentile = self._calc_percentile(input_uids[DATA_UID], threshold, level, condition)
        self._data_helper.put(output_uids[0], values=percentile['data'],
                              segment=percentile['@base_period'], level=str(threshold),
                              longitudes=percentile['@longitude_grid'], latitudes=percentile['@latitude_grid'],
                              times=percentile['@day_grid'], fill_value=percentile['@fill_value'],
                              meta=percentile['meta'])

        self.logger.info('Finished!')
