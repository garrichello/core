""" CalcGSL implements calculation of a spatial field of the growing season length based soleily on temperature.

    Input arguments:
        input_uids[0] -- average daily temperatures, WHOLE YEARS ONLY!
        input_uids[1] -- module parameters:
            Mode -- string, allowed values:
                'segment' -- for each segment
                'data' -- average over all segments
    Output arguments:
        output_uids[0] -- growing season length, data array of size:
            [segments, lats, lons] -- for Mode == 'segment'
            [lats, lons] -- for Mode == 'data'
"""

from copy import deepcopy
import numpy as np
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import kelvin_to_celsius
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1
DATA_UID = 0
DEFAULT_VALUES = {'Mode': 'data'}
THRESHOLD = 278.15  # 5 degC in K

class CalcGSL(Calc):
    """ Provides calculation of a spatial field of the growing season length values.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def _calc_gsl(self, values, threshold):
        """ Calculates GSL for the given values"""

        data_shape = values.shape[1:]
        gsl_cnt = ma.zeros(data_shape)
        warm_cnt = np.zeros(data_shape)
        cold_cnt = np.zeros(data_shape)
        increment = np.zeros(data_shape)
        for i, arr in enumerate(values):
            if i < 183:  # Take the first half of an year.
                mask = arr > threshold
                warm_cnt += mask  # Count warm days.
                warm_cnt *= mask  # Reset counter of warm days on a cold one.
                increment = ma.logical_or(increment, (warm_cnt == 5))  # Search for cells with 5 consecutive warm days.
            else:  # Take the second part of an year.
                mask = arr < threshold
                cold_cnt += mask  # Count cold days.
                cold_cnt *= mask  # Reset counter of cold days on a warm one.
                increment = ma.logical_and(increment, ~(cold_cnt == 5))  # Search for cells with 5 consecutive cold days.
            gsl_cnt += increment  # Count days inside GSL at each cell.

        gsl_cnt.mask = values.mask[0]  # Take source mask.

        return gsl_cnt

    def run(self):
        """ Main method of the class. Reads data array, process them and returns results. """

        print('(CalcGSL::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(CalcGSL::run) No input arguments!'

        # Get parameters
        parameters = None
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:  # If parameters are given.
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
        calc_mode = self._get_parameter('Mode', parameters, DEFAULT_VALUES)

        print('(CalcGSL::run) Calculation mode: {}'.format(calc_mode))

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcGSL::run) No output arguments!'

        # Get time segments and levels
        time_segments = self._data_helper.get_segments(input_uids[DATA_UID])
        vertical_levels = self._data_helper.get_levels(input_uids[DATA_UID])

        data_func = ma.mean  # For calc_mode == 'data' we calculate mean over all segments.

        # Convert degK to degC if data are given in C
        threshold = THRESHOLD
        data_info = self._data_helper.get_data_info(input_uids[DATA_UID])
        if data_info['description']['@units'] == 'C':
            threshold = kelvin_to_celsius(threshold)

        # Main loop
        for level in vertical_levels:
            all_segments_data = []
            for segment in time_segments:
                # Read data
                data = self._data_helper.get(input_uids[DATA_UID], segments=segment, levels=level)
                values = data['data'][level][segment['@name']]['@values']

                # Calculate the GSL.
                one_segment_data = self._calc_gsl(values, threshold)

                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if calc_mode == 'segment':
                    self._data_helper.put(output_uids[0], values=one_segment_data, level=level, segment=segment,
                                          longitudes=data['@longitude_grid'],
                                          latitudes=data['@latitude_grid'],
                                          fill_value=data['@fill_value'],
                                          meta=data['meta'])
                elif calc_mode == 'data':
                    all_segments_data.append(one_segment_data)
                else:
                    print('(CalcGSL::run) Error! Unknown calculation mode: \'{}\''.format(calc_mode))
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
                                      fill_value=data['@fill_value'], meta=data['meta'])

        print('(CalcGSL::run) Finished!')
