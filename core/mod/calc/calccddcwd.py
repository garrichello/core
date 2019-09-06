""" CalcCDDCWD implements calculation of a spatial field of the maximum length of dry (CDD) or wet (CWD) spell.

    Input arguments:
        input_uids[0] -- daily precipitation amount
        input_uids[1] -- module parameters:
            Type -- string, allowed values:
                'cdd' -- calculate maximum length of dry spell
                'cwd' -- calculate maximum length of wet spell
            Mode -- string, allowed values:
                'segment' -- for each segment
                'data' -- average over all segments
    Output arguments:
        output_uids[0] -- growing season length, data array of size:
            [segments, lats, lons] -- for Mode == 'segment'
            [lats, lons] -- for Mode == 'data'
"""

import operator
from copy import deepcopy
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1
DATA_UID = 0
DEFAULT_VALUES = {'Type': 'cdd', 'Mode': 'data'}
THRESHOLD = 1  # 1mm

class CalcCDDCWD(Calc):
    """ Provides calculation of a spatial field of the CDD/CWD.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def _calc_cddcwd(self, values, threshold, calc_type):
        """ Calculates maximum number of consecutive days with daily values (precipitation) < 1mm or >= 1mm (CDD or CWD).
        """

        if calc_type == 'cdd':
            cmp_func = operator.lt
        elif calc_type == 'cwd':
            cmp_func = operator.ge
        else:
            print('(CalcCDDCWD::run) Unknown calculation type: {}. Aborting!'.format(calc_type))
            raise ValueError

        data_shape = values.shape[1:]
        cnt = ma.zeros(data_shape)
        max_cnt = ma.zeros(data_shape)
        for arr in values:
            mask = cmp_func(arr, threshold)
            cnt += mask  # Count consecutive days.
            cnt *= mask  # Reset counter where condition does not meet.
            max_cnt = ma.max(ma.stack((max_cnt, cnt)), axis=0)

        max_cnt.mask = values[0].mask  # Restore mask from the original data array.

        return max_cnt

    def run(self):
        """ Main method of the class. Reads data array, process them and returns results. """

        print('(CalcCDDCWD::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(CalcCDDCWD::run) No input arguments!'

        # Get parameters
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:  # If parameters are given.
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
            calc_type = self._get_parameter('Type', parameters, DEFAULT_VALUES)
            calc_mode = self._get_parameter('Mode', parameters, DEFAULT_VALUES)

        print('(CalcCDDCWD::run) Calculation type: {}'.format(calc_type.upper()))
        print('(CalcCDDCWD::run) Calculation mode: {}'.format(calc_mode))

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcCDDCWD::run) No output arguments!'

        # Get time segments and levels
        time_segments = self._data_helper.get_segments(input_uids[DATA_UID])
        vertical_levels = self._data_helper.get_levels(input_uids[DATA_UID])

        data_func = ma.max  # For calc_mode == 'data' we calculate max over all segments.

        # Convert from m to mm if data are given in m
        threshold = THRESHOLD
        data_info = self._data_helper.get_data_info(input_uids[DATA_UID])
        if data_info['description']['@units'] == 'm':
            threshold *= 1000

        # Main loop
        for level in vertical_levels:
            all_segments_data = []
            for segment in time_segments:
                # Read data
                data = self._data_helper.get(input_uids[DATA_UID], segments=segment, levels=level)
                values = data['data'][level][segment['@name']]['@values']

                # Calculate the CDD/CWD.
                one_segment_data = self._calc_cddcwd(values, threshold, calc_type)

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
                    print('(CalcCDDCWD::run) Error! Unknown calculation mode: \'{}\''.format(calc_mode))
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

        print('(CalcCDDCWD::run) Finished!')
