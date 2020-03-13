"""Class CalcNormals provides methods for calculation of climate normals for the 30-year Base period.
"""

import datetime
import numpy as np

from core.base.dataaccess import DataAccess
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1
DEFAULT_VALUES = {'Mode': 'data'}

class CalcNormals(Calc):
    """ Performs calculation of climate normals of values.

    """

    def __init__(self, data_helper: DataAccess):
        super().__init__()
        self._data_helper = data_helper

    def _calc_normals(self, uid, level, calc_mode):
        """ Calculates climate normals at given vertical levels for a given time period.
        Arguments:
            uid -- UID of input dataset.
        Returns:
            normals -- [time, lat, lon] masked array of climate normals.
        """

        # Get time segments and levels and data info.
        time_segment = self._data_helper.get_segments(uid)[0]  # Only the first time segment is taken.
        data_info = self._data_helper.get_data_info(uid)

        start_date = datetime.datetime.strptime(time_segment['@beginning'], '%Y%m%d%H')
        end_date = datetime.datetime.strptime(time_segment['@ending'], '%Y%m%d%H')
        years = [start_date.year + i for i in range(end_date.year - start_date.year + 1)]
        
        normals = {}
        all_segments_data = []

        if calc_mode == 'day':
            segment_start = datetime.datetime(1, start_date.month, start_date.day, start_date.hour)
            segment_end = datetime.datetime(1, end_date.month, end_date.day, end_date.hour)
            dates_delta = segment_end - segment_start + datetime.timedelta(days=1)  # Days in the segment.
            days_grid = [segment_start + datetime.timedelta(days=i) for i in range(dates_delta.days)]  # Days of the segment.
            segments_grid = days_grid
            
            # For each day of the year (segment) this day for all years (30).
            # Concatenate for 30 years to obtain 30 lon-lat grids.
            # Calculate normals along time axis for each cell of the grid to get 2-D grid.
            # Make a Masked array using mask from one of the 1-day arrays.
            # We suppose that the mask is the same for all lon-lat grids along the time axis.
            # Concatenate 2-D grids for all days along time axis to get [time, lat, lon] result array.

            for day in days_grid:
                segments = []
                for year in years:
                    day_i = datetime.datetime(year, day.month, day.day, day.hour, day.minute)
                    one_day = {}  # 1-day segment to read.
                    one_day['@beginning'] = day_i.strftime('%Y%m%d%H')
                    one_day['@ending'] = day_i.strftime('%Y%m%d23')
                    one_day['@name'] = 'Year {}'.format(year)
                    segments.append(one_day)
                result = self._data_helper.get(uid, segments=segments, levels=level)
                data = np.ma.stack(
                    [result['data'][level]['Year {}'.format(year)]['@values'] for year in years])
                all_segments_data.append(np.ma.mean(data, axis=0))
        
        elif calc_mode == 'month':
            months = [start_date.month + i for i in range(end_date.month - start_date.month + 1)] # Months of the segment.
            months_grid = [datetime.datetime(1, mi, 14, 12) if mi == 2 else datetime.datetime(1, mi, 16, 0) for mi in months]
            segments_grid = months_grid

            for month in months_grid:
                segments = []
                for year in years:
                    month_i = datetime.datetime(year, month.month, month.day, month.hour, month.minute)
                    one_month = {}  # 1-month segment to read.
                    one_month['@beginning'] = month_i.strftime('%Y%m%d%H')
                    one_month['@ending'] = month_i.strftime('%Y%m%d23')
                    one_month['@name'] = 'Year {}'.format(year)
                    segments.append(one_month)
                result = self._data_helper.get(uid, segments=segments, levels=level)
                data = np.ma.stack(
                    [result['data'][level]['Year {}'.format(year)]['@values'] for year in years])
                all_segments_data.append(np.ma.mean(data, axis=0))
        else:
            self.logger.error('Error! Unknown calculation mode value: \'%s\'', calc_mode)
            raise ValueError 

        normals_data = np.stack(all_segments_data)  # Stack to array.
        mask_0 = result['data'][level]['Year {}'.format(years[0])]['@values'].mask  # lon-lat mask.
        mask_shape = [1] * (mask_0.ndim + 1)  # New shape of the mask.
        mask_shape[0] = len(segments_grid)  # Set it to be (n_days, 1, 1) or (n_days, 1).
        mask = np.tile(mask_0, mask_shape)  # Generate a mask to conform data dimensions.
        normals['data'] = np.ma.MaskedArray(normals_data, mask=mask, fill_value=result['@fill_value'])

        normals['@base_period'] = time_segment
        normals['@day_grid'] = segments_grid
        normals['@longitude_grid'] = result['@longitude_grid']
        normals['@latitude_grid'] = result['@latitude_grid']
        normals['@fill_value'] = result['@fill_value']
        normals['meta'] = result['meta']
        normals['meta']['varname'] = data_info['variable']['@name'] + '_normals'
        normals['meta']['time_long_name'] = 'Calendar day of the year'

        return normals

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        self.logger.info('Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, 'Error! No input arguments!'

        level = self._data_helper.get_levels(input_uids[0])[0]  # Only the first vertical level is taken.

        # Get parameters
        parameters = None
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:  # If parameters are given.
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
       
        calc_mode = self._get_parameter('Mode', parameters, DEFAULT_VALUES)
        self.logger.info('Calculation mode: %s', calc_mode)
        
        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, 'Error! No output arguments!'

        # Calculate normals
        normals = self._calc_normals(input_uids[0], level, calc_mode)
        self._data_helper.put(output_uids[0], values=normals['data'],
                              segment=normals['@base_period'], level=level,
                              longitudes=normals['@longitude_grid'], latitudes=normals['@latitude_grid'],
                              times=normals['@day_grid'], fill_value=normals['@fill_value'],
                              meta=normals['meta'])

        self.logger.info('Finished!')
