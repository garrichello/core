""" Base class CalcBasicStat provides basic statistical methods methods for time sets analysis
"""

from copy import copy
import numpy.ma as ma
import itertools

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1
DEFAULT_MODE = 'data'

class CalcBasicStat(Calc):
    """ Provides basic statistical analysis.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def _data_time_key(self, data_time):
        return data_time[1].date()

    def _run(self, calc_mode):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(CalcBasicStat::run) Started!')
        print('(CalcBasicStat::run) Calculation mode: {}'.format(calc_mode))

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(CalcBasicStat::run) No input arguments!'

        # Get parameters
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
            if parameters.get(calc_mode) is None:  # If the calculation mode is not set...
                parameters[calc_mode] = DEFAULT_MODE  # give it a default value.
        else:
            parameters = {}
            parameters[calc_mode] = DEFAULT_MODE

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcBasicStat::run) No output arguments!'

        # Get time segments and levels
        time_segments = self._data_helper.get_segments(input_uids[0])
        vertical_levels = self._data_helper.get_levels(input_uids[0])

        # Make desired statistical function shortcut.
        if calc_mode == 'timeMean':
            stat_func = ma.mean
        elif calc_mode == 'timeMin':
            stat_func = ma.min
        elif calc_mode == 'timeMax':
            stat_func = ma.max


        for level in vertical_levels:
            all_segments_data = []
            for segment in time_segments:
                one_segment_time_grid = []
                
                # Get data
                result = self._data_helper.get(input_uids[0], segments=segment, levels=level)

                # Daily statistics.
                if parameters[calc_mode] == 'day':
                    one_segment_data = []
                    data_time_iter = itertools.zip_longest(result['data'][level][segment['@name']]['@values'],
                                                           result['data'][level][segment['@name']]['@time_grid'])
                    for date_key, group in itertools.groupby(data_time_iter, key=self._data_time_key):
                        group_data = []
                        for data, _ in group:
                            group_data.append(data)
                        group_data = ma.stack(group_data)
                        one_segment_time_grid.append(date_key)

                        # Calulate time statistics for a current time group (day)
                        one_segment_data.append(stat_func(group_data, axis=0))
                    
                    one_segment_data = ma.stack(one_segment_data)

                # Calulate time statistics for a current time segment
                if (parameters[calc_mode] == 'data') or (parameters[calc_mode] == 'segment'):
                    one_segment_data = stat_func(result['data'][level][segment['@name']]['@values'], axis=0)
                    one_segment_time_grid.append(result['data'][level][segment['@name']]['@time_grid'][0])

                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if (parameters[calc_mode] == 'day') or (parameters[calc_mode] == 'segment'): 
                    self._data_helper.put(output_uids[0], values=one_segment_data, level=level, segment=segment,
                                          longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                          times=one_segment_time_grid, fill_value=result['@fill_value'], meta=result['meta'])
                elif parameters[calc_mode] == 'data': 
                    all_segments_data.append(one_segment_data)

            # For data-wise analysis analyse segments analyses :)
            if parameters[calc_mode] == 'data': 
                data_out = stat_func(ma.stack(all_segments_data), axis=0)

                # Make a global segment covering all input time segments
                full_range_segment = copy(time_segments[0])  # Take the beginning of the first segment...
                full_range_segment['@ending'] = time_segments[-1]['@ending']  # and the end of the last one.
                full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

                self._data_helper.put(output_uids[0], values=data_out, level=level, segment=full_range_segment,
                                      longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                      fill_value=result['@fill_value'], meta=result['meta'])

        print('(CalcBasicStat::run) Finished!')
