""" Class cvcCalcTiMean provides methods for time mean calculation
"""

from copy import copy
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1
CALC_MODE = 'timeMean'
DEFAULT_MODE = 'data'

class cvcCalcTiMean():
    """ Performs calculation of time averaged values.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(cvcCalcTiMean::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(cvcCalcTiMean::run) No input arguments!'

        # Get parameters
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
            if parameters.get(CALC_MODE) is None:  # If the calculation mode is not set...
                parameters[CALC_MODE] = DEFAULT_MODE  # give it a default value.

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(cvcCalcTiMean::run) No output arguments!'

        # Get time segments and levels
        time_segments = self._data_helper.get_segments(input_uids[0])
        vertical_levels = self._data_helper.get_levels(input_uids[0])

        for level in vertical_levels:
            all_segments_means = []
            for segment in time_segments:
                # Get data
                result = self._data_helper.get(input_uids[0], segments=segment, levels=level)

                # Calulate time averaged values for a current time segment
                one_segment_mean = result['data'][level][segment['@name']]['@values'].mean(axis=0)

                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if parameters[CALC_MODE] == 'segment': 
                    self._data_helper.put(output_uids[0], values=one_segment_mean, level=level, segment=segment,
                                          longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                          fill_value=result['@fill_value'], meta=result['meta'])
                else:
                    all_segments_means.append(one_segment_mean)

            # For data-wise averaging average segments averages :)
            if parameters[CALC_MODE] == 'data':
                data_mean = ma.stack(all_segments_means).mean(axis=0)

                # Make a global segment covering all input time segments
                full_range_segment = copy(time_segments[0])  # Take the beginning of the first segment...
                full_range_segment['@ending'] = time_segments[-1]['@ending']  # and the end of the last one.
                full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

                self._data_helper.put(output_uids[0], values=data_mean, level=level, segment=full_range_segment,
                                      longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                      fill_value=result['@fill_value'], meta=result['meta'])

        print('(cvcCalcTiMean::run) Finished!')
