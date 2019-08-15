""" CalcExceedance implements calculation of a spatial field of cold/warm nights/days values for time series of data.
"""

from copy import copy
import itertools
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 3
INPUT_PARAMETERS_INDEX = 2
NORMALS_UID = 0
STUDY_UID = 1
TYPE_PARAMETER_NAME = 'Type'
THRESHOLD_PARAMETER_NAME = 'Threshold'
MODE_PARAMETER_NAME = 'Mode'
DEFAULT_TYPE = 'duration'
DEFAULT_THRESHOLD = 'low'
DEFAULT_MODE = 'single'

class CalcExceedance(Calc):
    """ Provides calculation of a spatial field of cold/warm nights/days values for time series of data.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(CalcExceedance::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(CalcExceedance::run) No input arguments!'

        # Get parameters
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
            if parameters.get(TYPE_PARAMETER_NAME) is None:  # If the calculation type is not set...
                parameters[TYPE_PARAMETER_NAME] = DEFAULT_TYPE  # give it a default value.
            if parameters.get(THRESHOLD_PARAMETER_NAME) is None:  # If the exceedance threshold is not set...
                parameters[THRESHOLD_PARAMETER_NAME] = DEFAULT_THRESHOLD  # give it a default value.
            if parameters.get(MODE_PARAMETER_NAME) is None:  # If the calculation mode is not set...
                parameters[MODE_PARAMETER_NAME] = DEFAULT_MODE  # give it a default value.            
        else:
            parameters = {TYPE_PARAMETER_NAME: DEFAULT_TYPE,
                          THRESHOLD_PARAMETER_NAME: DEFAULT_THRESHOLD,
                          MODE_PARAMETER_NAME: DEFAULT_MODE}

        print('(CalcExceedance::run) Calculation type {}'.format(parameters[TYPE_PARAMETER_NAME]))
        print('(CalcExceedance::run) Exceedance threshold {}'.format(parameters[THRESHOLD_PARAMETER_NAME]))
        print('(CalcExceedance::run) Calculation mode {}'.format(parameters[MODE_PARAMETER_NAME]))

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcExceedance::run) No output arguments!'

        # Get time segments and levels
        study_time_segments = self._data_helper.get_segments(input_uids[STUDY_UID])
        study_vertical_levels = self._data_helper.get_levels(input_uids[STUDY_UID])
        # Normals time segments should be set for year 1 (as set in a pdftails file)
        normals_time_segments = copy(study_time_segments)
        for segment in normals_time_segments:
            segment['@beginning'] = '0001' + segment['@beginning'][4:]
            segment['@ending'] = '0001' + segment['@ending'][4:]

        # Read normals data
        normals_data = self._data_helper.get(NORMALS_UID, segments=normals_time_segments)
        study_data = self._data_helper.get(STUDY_UID, segments=study_time_segments)

        # Make desired statistical function shortcut for segment and final processing .
        if calc_mode == 'timeMean':
            seg_stat_func = ma.mean
            final_stat_func = ma.mean
        elif calc_mode == 'timeMin':
            seg_stat_func = ma.min
            final_stat_func = ma.min
        elif calc_mode == 'timeMax':
            seg_stat_func = ma.max
            final_stat_func = ma.max
        elif calc_mode == 'timeMeanPrec':
            seg_stat_func = ma.sum
            final_stat_func = ma.mean

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

                # Calulate time statistics for a current time segment
                if (parameters[calc_mode] == 'data') or (parameters[calc_mode] == 'segment'):
                    one_segment_data = seg_stat_func(result['data'][level][segment['@name']]['@values'], axis=0)
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
                data_out = final_stat_func(ma.stack(all_segments_data), axis=0)

                # Make a global segment covering all input time segments
                full_range_segment = copy(time_segments[0])  # Take the beginning of the first segment...
                full_range_segment['@ending'] = time_segments[-1]['@ending']  # and the end of the last one.
                full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

                self._data_helper.put(output_uids[0], values=data_out, level=level, segment=full_range_segment,
                                      longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                      fill_value=result['@fill_value'], meta=result['meta'])

        print('(CalcExceedance::run) Finished!')
