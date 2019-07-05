""" Class cvcCalcTiMean provides methods for time mean calculation
"""

from copy import copy
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1

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

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(cvcCalcTiMean::run) No output arguments!'

        # Get data for all time segments and levels at once
        time_segments = self._data_helper.get_segments(input_uids[0])
        vertical_levels = self._data_helper.get_levels(input_uids[0])
        result = self._data_helper.get(input_uids[0], segments=time_segments, levels=vertical_levels)

        for level in vertical_levels:
            all_segments_means = []
            for segment in time_segments:
                # Let's calulate time averaged values
                one_segment_mean = result['data'][level][segment['@name']]['@values'].mean(axis=0)
                if parameters['timeMean'] == 'segment':
                    self._data_helper.put(output_uids[0], values=one_segment_mean, level=level, segment=segment, longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'], fill_value=result['@fill_value'], meta=result['meta'])
                else:
                    all_segments_means.append(one_segment_mean)

            if parameters['timeMean'] == 'data':
                data_mean = ma.stack(all_segments_means).mean(axis=0)
                # Make a global segment covering all input time segments
                full_range_segment = copy(time_segments[0])
                full_range_segment['@ending'] = time_segments[-1]['@ending']
                full_range_segment['@name'] = 'GlobalSeg'
                self._data_helper.put(output_uids[0], values=data_mean, level=level, segment=full_range_segment,
                                      longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                      fill_value=result['@fill_value'], meta=result['meta'])

        print('(cvcCalcTiMean::run) Finished!')
