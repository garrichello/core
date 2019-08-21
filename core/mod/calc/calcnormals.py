"""Class CalcNormals provides methods for calculation of 10th and 90th normals
of daily maximum temperature values for 5 consecutive days window of the 30-year Base period.
"""

import datetime
import numpy as np

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2

class CalcNormals(Calc):
    """ Performs calculation of n-th normals of daily maximum temperatures.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def _calc_normals(self, uid, level):
        """ Calculates climate normals at given vertical levels for a given time period.
        Arguments:
            uid -- UID of input dataset.
        Returns:
            normals -- [time, lat, lon] masked array of climate normals.
        """

        # Get time segments and levels and data info.
        time_segment = self._data_helper.get_segments(uid)[0]  # Only the first time segment is taken.
        data_info = self._data_helper.get_data(uid)

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
        normals = {}
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
            all_days_data.append(np.ma.mean(data, axis=0))

            normals_data = np.stack(all_days_data, axis=0)  # Stack to array.
            mask = result['data'][level]['Year {}'.format(years[0])]['@values'].mask[0]  # lon-lat mask.
            normals['data'] = np.ma.MaskedArray(normals_data, mask=mask, fill_value=result['@fill_value'])

        normals['@base_period'] = time_segment
        normals['@day_grid'] = days
        normals['@longitude_grid'] = result['@longitude_grid']
        normals['@latitude_grid'] = result['@latitude_grid']
        normals['@fill_value'] = result['@fill_value']
        normals['meta'] = result['meta']
        normals['meta']['varname'] = data_info['variable']['@name'] + '_normals'
        normals['meta']['time_long_name'] = 'Calendar day of the year'

        return normals

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(CalcNormals::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(CalcNormals::run) No input arguments!'

        level = self._data_helper.get_levels(input_uids[0])[0]  # Only the first vertical level is taken.

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcNormals::run) No output arguments!'

        # Calculate normals
        normals = self._calc_normals(input_uids[0], level)
        self._data_helper.put(output_uids[0], values=normals['data'],
                              segment=normals['@base_period'], level=level,
                              longitudes=normals['@longitude_grid'], latitudes=normals['@latitude_grid'],
                              times=normals['@day_grid'], fill_value=normals['@fill_value'],
                              meta=normals['meta'])

        print('(CalcNormals::run) Finished!')
