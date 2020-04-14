""" Base class CalcBasicStat provides basic statistical methods methods for time sets analysis
"""

from copy import copy, deepcopy
import itertools
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1

class CalcBasicStat(Calc):
    """ Provides basic statistical analysis.

    """

    def __init__(self, data_helper: DataAccess):
        super().__init__()
        self._data_helper = data_helper

    def _data_time_key(self, data_time):
        return data_time[1].date()

    def _run(self, calc_mode):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        self.logger.info('Started!')
        self.logger.info('Calculation mode: %s', calc_mode)

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(CalcBasicStat::run) No input arguments!'

        # Get parameters
        parameters = None
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])

        # Check parameters
        if not calc_mode in parameters:
            self.logger.error('Error! No parameter \'%s\' in module parameters! Check task-file!', calc_mode)
            raise ValueError

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcBasicStat::run) No output arguments!'

        # Get time segments and levels
        time_segments = self._data_helper.get_segments(input_uids[0])
        vertical_levels = self._data_helper.get_levels(input_uids[0])

        data_info = self._data_helper.get_data_info(input_uids[0])
        description = deepcopy(data_info['description'])

        # Make desired statistical function shortcut for segment and final processing .
        if calc_mode == 'timeMean':
            seg_stat_func = ma.mean
            final_stat_func = ma.mean
            final_title = 'Average of ' + description['@title'].lower()
            final_name = description['@name'] + '_mean'
        elif calc_mode == 'timeMin':
            seg_stat_func = ma.min
            final_stat_func = ma.min
            final_title = 'Minimum of ' + description['@title'].lower()
            final_name = description['@name'] + '_min'
        elif calc_mode == 'timeMax':
            seg_stat_func = ma.max
            final_stat_func = ma.max
            final_title = 'Maximum of ' + description['@title'].lower()
            final_name = description['@name'] + '_max'
        elif calc_mode == 'timeMeanPrec':
            seg_stat_func = ma.sum
            final_stat_func = ma.mean
            final_title = 'Average sum of ' + description['@title'].lower()
            final_name = description['@name'] + '_meansum'

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
                        one_segment_data.append(seg_stat_func(group_data, axis=0))

                    one_segment_data = ma.stack(one_segment_data)

                # Calculate time statistics for a current time segment
                if (parameters[calc_mode] == 'data') or (parameters[calc_mode] == 'segment'):
                    if len(result['data'][level][segment['@name']]['@time_grid']) > 1:
                        one_segment_data = seg_stat_func(result['data'][level][segment['@name']]['@values'], axis=0)
                    else:
                        one_segment_data = result['data'][level][segment['@name']]['@values']
                    mid_time = result['data'][level][segment['@name']]['@time_grid'][0] + \
                               (result['data'][level][segment['@name']]['@time_grid'][-1] - \
                                result['data'][level][segment['@name']]['@time_grid'][0]) / 2
                    one_segment_time_grid.append(mid_time)

                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if (parameters[calc_mode] == 'day') or (parameters[calc_mode] == 'segment'):
                    self._data_helper.put(output_uids[0], values=one_segment_data, level=level, segment=segment,
                                          longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                          times=one_segment_time_grid, fill_value=result['@fill_value'], meta=result['meta'],
                                          description=description)
                elif parameters[calc_mode] == 'data':
                    all_segments_data.append(one_segment_data)

            # For data-wise analysis analyse segments analyses :)
            if parameters[calc_mode] == 'data':
                data_out = final_stat_func(ma.stack(all_segments_data), axis=0)

                # Make a global segment covering all input time segments
                full_range_segment = copy(time_segments[0])  # Take the beginning of the first segment...
                full_range_segment['@ending'] = time_segments[-1]['@ending']  # and the end of the last one.
                full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

                # Correct title and name
                if final_title is not None:
                    description['@title'] = final_title
                if final_name is not None:
                    description['@name'] = final_name

                self._data_helper.put(output_uids[0], values=data_out, level=level, segment=full_range_segment,
                                      longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                      fill_value=result['@fill_value'], meta=result['meta'],
                                      description=description)

        self.logger.info('Finished!')
