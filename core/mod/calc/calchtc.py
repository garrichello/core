""" CalcHTC implements calculation of a spatial field of the Selyaninov's hydrothermal coefficient [Selyaninov, 1928].

    Input arguments:
        input_uids[0] -- daily temperature values
        input_uids[1] -- daily total precipitation values
        input_uids[2] -- module parameters:
            Mode -- string, allowed values:
                'segment' -- for each segment
                'data' -- maximum over all segments
            Threshold -- integer, threshold temperature (usually, 10 degC)
    Output arguments:
        output_uids[0] -- Selyaninov's hydrothermal coefficient, data array of size:
            [segments, lats, lons] -- for Mode == 'segment'
            [lats, lons] -- for Mode == 'data'
"""

from copy import deepcopy
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 3
INPUT_PARAMETERS_INDEX = 2
PRCP_DATA_UID = 0
TEMP_DATA_UID = 1
DEFAULT_VALUES = {'Mode': 'data', 'Threshold': 10}

class CalcHTC(Calc):
    """ Provides calculation of a spatial field of the Selyaninov's hydrothermal coefficient values for time series of data.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def _calc_htc(self, prcp_values, temp_values, threshold):
        return 0

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(CalcHTC::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(CalcHTC::run) No input arguments!'

        # Get parameters
        parameters = None
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:  # If parameters are given.
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
        calc_mode = self._get_parameter('Mode', parameters, DEFAULT_VALUES)
        threshold = self._get_parameter('Threshold', parameters, DEFAULT_VALUES)

        print('(CalcHTC::run) Calculation mode: {}'.format(calc_mode))
        print('(CalcHTC::run) Threshold: {}'.format(threshold))

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcHTC::run) No output arguments!'

        # Get time segments and levels (only for maximum data, for minimum they must be the same)
        time_segments = self._data_helper.get_segments(input_uids[PRCP_DATA_UID])
        vertical_levels = self._data_helper.get_levels(input_uids[PRCP_DATA_UID])

        data_func = ma.mean  # For calc_mode == 'data' we calculate mean over all segments.

        for level in vertical_levels:
            all_segments_values = []
            for segment in time_segments:
                # Read data
                prcp_data = self._data_helper.get(input_uids[PRCP_DATA_UID], segments=segment, levels=level)
                prcp_values = prcp_data['data'][level][segment['@name']]['@values']
                temp_data = self._data_helper.get(input_uids[TEMP_DATA_UID], segments=segment, levels=level)
                temp_values = temp_data['data'][level][segment['@name']]['@values']

                # Perform calculation for the current time segment.
                one_segment_values = self._calc_htc(prcp_values, temp_values, threshold)

                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if calc_mode == 'segment':
                    self._data_helper.put(output_uids[0], values=one_segment_values, level=level, segment=segment,
                                          longitudes=prcp_data['@longitude_grid'],
                                          latitudes=prcp_data['@latitude_grid'],
                                          fill_value=prcp_data['@fill_value'],
                                          meta=prcp_data['meta'])
                elif calc_mode == 'data':
                    all_segments_values.append(one_segment_values)
                else:
                    print('(CalcHTC::run) Error! Unknown calculation mode: \'{}\''.format(calc_mode))
                    raise ValueError

            # For data-wise analysis analyse segments analyses :)
            if calc_mode == 'data':
                values_out = data_func(ma.stack(all_segments_values), axis=0)

                # Make a global segment covering all input time segments
                full_range_segment = deepcopy(time_segments[0])  # Take the beginning of the first segment...
                full_range_segment['@ending'] = time_segments[-1]['@ending']  # and the end of the last one.
                full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

                self._data_helper.put(output_uids[0], values=values_out, level=level, segment=full_range_segment,
                                      longitudes=prcp_data['@longitude_grid'], latitudes=prcp_data['@latitude_grid'],
                                      fill_value=prcp_data['@fill_value'], meta=prcp_data['meta'])

        print('(CalcHTC::run) Finished!')
