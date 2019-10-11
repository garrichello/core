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

import operator
from copy import deepcopy
import datetime

import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import print, celsius_to_kelvin  # pylint: disable=W0622
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

    def _calc_half_htc(self, prcp_values, temp_values, threshold, cmp_func):
        """ Calculates Selyaninov's hydrothermal coeffcient for only a half of an year.
        Arguments:
            prcp_values -- daily total precipitation values
            temp_values -- daily mean temperature values
            threshold -- threshold temperature (10C, by default)
            cmp_func -- comparative function: 
                operator.ge -- to process the first half of the year
                operator.lt -- to process the second half of the year
        Returns:
            result -- array of Selyaninov's hydrothermal coefficient values
        """
        # The idea is to subtract threshold from temperature values
        #  and to catch cases when values change sign from negative to positive (a transition)
        #  i.e., pass x-axis upwards.
        # Each crossing correspond to a possible beginning of vegetation period.
        # We sum values between crossings (segments) and analyse sums according to (Ped', 1951).

        # Define a function to detect False->True transition.
        trans = lambda a, b: ma.logical_and(ma.logical_not(a), b)

        # Create shape for new arrays without time dimmension.
        dims = list(temp_values.shape)
        _ = dims.pop(0)

        # Define some useful arrays.
        temp_sums_1 = ma.zeros(dims)  # Temperature sums for the first segment.
        temp_sums_2 = ma.zeros(dims)  # Temperature sums for the second segment.
        temp_sums_3 = ma.zeros(dims)  # Temperature sums for the third segment.
        temp_total = ma.zeros(dims)   # Temperature total sum for a vegetation period.
        prcp_sums_1 = ma.zeros(dims)  # Precipitation sums for the first segment.
        prcp_sums_2 = ma.zeros(dims)  # Precipitation sums for the second segment.
        prcp_sums_3 = ma.zeros(dims)  # Precipitation sums for the third segment.
        prcp_total = ma.zeros(dims)   # Precipitation total sum for a vegetation period.

        trans_mask_1 = ma.zeros(dims)  # Mask of cells in the first segment state
                                      #  (i.e., where the first transition was detected).
        trans_mask_2 = ma.zeros(dims)  # Mask of cells in the second segment state.
        trans_mask_3 = ma.zeros(dims)  # Mask of cells in the third segment state.
        cur_trans = ma.zeros(dims)  # Matrix of upward transitions at current time step.
        prev_pos_mask = ma.zeros(dims)  # Matrix of previous positive mask state.

        # Search for upward transitions and calculate sums.
        for cur_temp, cur_prcp in zip(temp_values, prcp_values):
            temp_deviation = cur_temp - threshold
            temp_pos_mask = cmp_func(temp_deviation, 0)  # Mask of positive values.
            cur_trans = trans(prev_pos_mask, temp_pos_mask)  # Detect negative->positive transition.
            prev_pos_mask = temp_pos_mask  # Store current positive mask for the next iteration.
            # Turn on the third state, if a transitions is detected and the second state is on.
            trans_mask_3 = ma.logical_and(cur_trans, trans_mask_2)
            # Turn on the second state, if a transitions is detected and the first state is on.
            trans_mask_2 = ma.logical_and(cur_trans, trans_mask_1)
            # Turn on the first state, if a transitions is detected or leave it as is.
            trans_mask_1 = ma.logical_or(cur_trans, trans_mask_1)
            # Sum values in the first segment.
            seg_1_mask = ma.logical_and(trans_mask_1, ma.logical_not(trans_mask_2))
            temp_sums_1[seg_1_mask] += temp_deviation[seg_1_mask]
            prcp_sums_1[seg_1_mask] += cur_prcp[seg_1_mask]
            # Sum values in the second segment.
            seg_2_mask = ma.logical_and(trans_mask_2, ma.logical_not(trans_mask_3))
            temp_sums_2[seg_2_mask] += temp_deviation[seg_2_mask]
            prcp_sums_2[seg_2_mask] += cur_prcp[seg_2_mask]
            # Sum values in the third segment (in fact, everything after the second one).
            temp_sums_3[trans_mask_3] += temp_deviation[trans_mask_3]
            prcp_sums_3[trans_mask_3] += cur_prcp[trans_mask_3]

        # Check Ped's conditions and calculate temperature sums for the vegetation period.
        # Create some intermediate sums.
        temp_sums_12 = temp_sums_1 + temp_sums_2
        temp_sums_23 = temp_sums_2 + temp_sums_3
        temp_sums_123 = temp_sums_12 + temp_sums_3
        # If vegetation period starts at the first transition point.
        start_at_seg_1 = ma.logical_and(temp_sums_1 >= 0, temp_sums_12 >= 0)
        temp_total[start_at_seg_1] = temp_sums_123[start_at_seg_1]
        prcp_total[start_at_seg_1] = prcp_sums_1[start_at_seg_1] + prcp_sums_2[start_at_seg_1] + prcp_sums_3[start_at_seg_1]
        # If vegetation period starts at the second transition point.
        start_at_seg_2 = ma.logical_and(temp_sums_1 < 0, temp_sums_2 >= 0)
        temp_total[start_at_seg_2] = temp_sums_23[start_at_seg_2]
        prcp_total[start_at_seg_2] = prcp_sums_2[start_at_seg_2] + prcp_sums_3[start_at_seg_2]
        # If vegetation period starts at the third transition point.
        start_at_seg_3 = ma.logical_or(ma.logical_and(temp_sums_1 < 0, temp_sums_2 < 0), 
                                       ma.logical_and(ma.logical_and(temp_sums_1 > 0, temp_sums_2 < 0), 
                                                      temp_sums_12 < 0))
        temp_total[start_at_seg_3] = temp_sums_3[start_at_seg_3]
        prcp_total[start_at_seg_3] = prcp_sums_3[start_at_seg_3]
        # Restore original temperature values from deviations.
        temp_total += threshold

        return (prcp_total, temp_total)

    def _calc_htc(self, prcp_values, temp_values, threshold, time_grid):
        """ Calculates Selyaninov's hydrothermal coeffcient.
        Arguments:
            prcp_values -- daily total precipitation values
            temp_values -- daily mean temperature values
            threshold -- threshold temperature (10C, by default)
        Returns:
            result -- array of Selyaninov's hydrothermal coefficient values
        """
        midyear = datetime.datetime(time_grid[0].year, 6, 15)
        first_half = time_grid < midyear
        second_half = time_grid >= midyear
        prcp_total_1, temp_total_1 = self._calc_half_htc(prcp_values[first_half], temp_values[first_half], threshold, operator.ge)
        prcp_total_2, temp_total_2 = self._calc_half_htc(prcp_values[second_half], temp_values[second_half], threshold, operator.lt)

        # Calculate HTC.
        result = 10 * (prcp_total_1 + prcp_total_2) / (temp_total_1 + temp_total_2)

        return result

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

        threshold = celsius_to_kelvin(threshold)

        print('(CalcHTC::run) Calculation mode: {}'.format(calc_mode))
        print('(CalcHTC::run) Threshold: {}'.format(threshold))

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcHTC::run) No output arguments!'

        # Get time segments and levels (only for maximum data, for minimum they must be the same)
        time_segments = self._data_helper.get_segments(input_uids[PRCP_DATA_UID])
        prcp_levels = self._data_helper.get_levels(input_uids[PRCP_DATA_UID])
        temp_levels = self._data_helper.get_levels(input_uids[TEMP_DATA_UID])

        data_func = ma.mean  # For calc_mode == 'data' we calculate mean over all segments.

        for prcp_level, temp_level in zip(prcp_levels, temp_levels):
            all_segments_values = []
            for segment in time_segments:
                # Read data
                prcp_data = self._data_helper.get(input_uids[PRCP_DATA_UID], segments=segment, levels=prcp_level)
                prcp_values = prcp_data['data'][prcp_level][segment['@name']]['@values']
                temp_data = self._data_helper.get(input_uids[TEMP_DATA_UID], segments=segment, levels=temp_level)
                temp_values = temp_data['data'][temp_level][segment['@name']]['@values']
                time_grid = prcp_data['data'][prcp_level][segment['@name']]['@time_grid']

                # Perform calculation for the current time segment.
                one_segment_values = self._calc_htc(prcp_values, temp_values, threshold, time_grid)

                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if calc_mode == 'segment':
                    self._data_helper.put(output_uids[0], values=one_segment_values, level=prcp_level, segment=segment,
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

                self._data_helper.put(output_uids[0], values=values_out, level=prcp_level, segment=full_range_segment,
                                      longitudes=prcp_data['@longitude_grid'], latitudes=prcp_data['@latitude_grid'],
                                      fill_value=prcp_data['@fill_value'], meta=prcp_data['meta'])

        print('(CalcHTC::run) Finished!')
