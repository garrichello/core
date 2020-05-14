"""Class CalcRnnmm provides methods for calculation of annual count of days when PRCP ≥ nn mm, where nn is a user-defined threshold.

    Input arguments:
        input_uids[0] -- total precipitation values
        input_uids[1] -- module parameters:
            Mode -- string, allowed values:
                'segment' -- for each segment
                'data' -- maximum over all segments
            Threshold -- integer, lower precipitation threshold [mm].

    Output arguments:
        output_uids[0] -- annual count of days array:
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
DEFAULT_VALUES = {'Mode': 'data', 'Threshold': 10}

class CalcRnnmm(Calc):
    """ Performs calculation of annual count of days when PRCP ≥ nn mm.

    """

    def __init__(self, data_helper: DataAccess):
        super().__init__()
        self._data_helper = data_helper

    def calc_rnnmm(self, values, time_grid, threshold):
        """ Calculates Rnnmm
        Arguments:
            values -- array of total precipitation
            time_grid -- time grid
            threshold -- number of days (n)
        Returns: Rxnday values
        """

        n_days = None
        it_all_data = groupby(zip(values, time_grid), key=lambda x: (x[1].day, x[1].month))
        for _, one_day_group in it_all_data:  # Iterate over daily groups.
            daily_sum = self._calc_daily_sum(one_day_group)
            mask = daily_sum > threshold
            if n_days is None:
                n_days = ma.zeros(daily_sum.shape)
            n_days += mask

        return n_days

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        self.logger.info('Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, 'Error! No input arguments!'

        # Get parameters
        parameters = None
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:  # If parameters are given.
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
        threshold = float(self._get_parameter('Threshold', parameters, DEFAULT_VALUES))
        calc_mode = self._get_parameter('Mode', parameters, DEFAULT_VALUES)

        self.logger.info('Threshold: %s', threshold)
        self.logger.info('Calculation mode: %s', calc_mode)

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, 'Error! No output arguments!'

        # Get time segments and levels
        time_segments = self._data_helper.get_segments(input_uids[DATA_UID])
        vertical_levels = self._data_helper.get_levels(input_uids[DATA_UID])

        data_func = ma.max  # For calc_mode == 'data' we calculate max over all segments.

        # Set result units.
        result_description = deepcopy(self._data_helper.get_data_info(input_uids[0])['description'])
        result_description['@title'] = 'Count of days with precipitation >= {} mm'.format(threshold)
        result_description['@units'] = 'days'

        # Main loop
        for level in vertical_levels:
            all_segments_data = []
            for segment in time_segments:
                # Read data
                data = self._data_helper.get(input_uids[DATA_UID], segments=segment, levels=level)
                values = data['data'][level][segment['@name']]['@values']
                time_grid = data['data'][level][segment['@name']]['@time_grid']

                one_segment_data = self.calc_rnnmm(values, time_grid, threshold)

                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if calc_mode == 'segment':
                    self._data_helper.put(output_uids[0], values=one_segment_data, level=level, segment=segment,
                                          longitudes=data['@longitude_grid'],
                                          latitudes=data['@latitude_grid'],
                                          fill_value=data['@fill_value'],
                                          description=result_description, meta=data['meta'])
                elif calc_mode == 'data':
                    all_segments_data.append(one_segment_data)
                else:
                    self.logger.error('Error! Unknown calculation mode: \'%s\'', calc_mode)
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
                                      fill_value=data['@fill_value'], meta=data['meta'],
                                      description=result_description)


        self.logger.info('Finished!')
