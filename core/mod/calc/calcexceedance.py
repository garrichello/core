""" CalcExceedance implements calculation of a spatial field of cold/warm nights/days values for time series of data.
"""

from copy import deepcopy
import operator
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
DEFAULT_TYPE = 'frequency'
DEFAULT_THRESHOLD = 'low'
DEFAULT_MODE = 'data'

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

        print('(CalcExceedance::run) Calculation type: {}'.format(parameters[TYPE_PARAMETER_NAME]))
        print('(CalcExceedance::run) Exceedance threshold: {}'.format(parameters[THRESHOLD_PARAMETER_NAME]))
        print('(CalcExceedance::run) Calculation mode: {}'.format(parameters[MODE_PARAMETER_NAME]))

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcExceedance::run) No output arguments!'

        # Get time segments and levels
        study_time_segments = self._data_helper.get_segments(input_uids[STUDY_UID])
        study_vertical_levels = self._data_helper.get_levels(input_uids[STUDY_UID])

        # Normals time segments should be set for year 1 (as set in a pdftails file)
        normals_time_segments = deepcopy(study_time_segments)
        percentile = self._data_helper.get_levels(input_uids[NORMALS_UID])[0]  # There should be only one level - percentile.
        for segment in normals_time_segments:
            segment['@beginning'] = '0001' + segment['@beginning'][4:]
            segment['@ending'] = '0001' + segment['@ending'][4:]

        # Read normals data
        normals_data = self._data_helper.get(input_uids[NORMALS_UID], segments=normals_time_segments)
        study_data = self._data_helper.get(input_uids[STUDY_UID], segments=study_time_segments)

        if parameters[THRESHOLD_PARAMETER_NAME] == 'low':
            comparison_func = operator.lt
        elif parameters[THRESHOLD_PARAMETER_NAME] == 'high':
            comparison_func = operator.gt
        else:
            print('(CalcExceedance::run) Error! Unknown threshold parameter value: \'{}\''.format(
                parameters[THRESHOLD_PARAMETER_NAME]))
            raise ValueError
        
        final_func = ma.max

        for level in study_vertical_levels:
            all_segments_data = []
            for segment in study_time_segments:
                normals_values = normals_data['data'][percentile][segment['@name']]['@values']
                study_values = study_data['data'][level][segment['@name']]['@values']

                # Calulate time statistics for the current time segment
                if parameters[TYPE_PARAMETER_NAME] == 'frequency':
                    one_segment_data = ma.mean(comparison_func(study_values, normals_values), axis=0) * 100
                
                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if (parameters[MODE_PARAMETER_NAME] == 'segment'):
                    one_segment_time_grid = study_data['data'][level][segment['@name']]['@time_grid']
                    self._data_helper.put(output_uids[0], values=one_segment_data, level=level, segment=segment,
                                          longitudes=study_data['@longitude_grid'], 
                                          latitudes=study_data['@latitude_grid'],
                                          times=one_segment_time_grid, fill_value=study_data['@fill_value'], 
                                          meta=study_data['meta'])
                elif parameters[MODE_PARAMETER_NAME] == 'data':
                    all_segments_data.append(one_segment_data)

            # For data-wise analysis analyse segments analyses :)
            if parameters[MODE_PARAMETER_NAME] == 'data':
                data_out = final_func(ma.stack(all_segments_data), axis=0)

                # Make a global segment covering all input time segments
                full_range_segment = deepcopy(study_time_segments[0])  # Take the beginning of the first segment...
                full_range_segment['@ending'] = study_time_segments[-1]['@ending']  # and the end of the last one.
                full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

                self._data_helper.put(output_uids[0], values=data_out, level=level, segment=full_range_segment,
                                      longitudes=study_data['@longitude_grid'], latitudes=study_data['@latitude_grid'],
                                      fill_value=study_data['@fill_value'], meta=study_data['meta'])

        print('(CalcExceedance::run) Finished!')
