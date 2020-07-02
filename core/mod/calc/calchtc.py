""" CalcHTC implements calculation of a spatial field of the Selyaninov's hydrothermal coefficient [Selyaninov, 1928] or Ped drought index [Ped, 1975].
    
    If parameter 'HTC' equal 'Selyaninov':
    Input arguments:
        input_uids[0] -- daily total precipitation values
        input_uids[1] -- daily temperature values
        input_uids[2] -- module parameters:
            Mode -- string, allowed values:
                'segment' -- for each segment
                'data' -- mean over all segments
            Threshold -- integer, threshold temperature (usually, 10 degC)
    
    Output arguments:
        output_uids[0] -- Selyaninov's hydrothermal coefficient, data array of size:
            [segments, lats, lons] -- for Mode == 'segment'
            [lats, lons] -- for Mode == 'data'

    If parameter 'HTC' equal 'Ped':
    Input arguments:
        input_uids[0] -- monthly total precipitation values
        input_uids[1] -- monthly air temperature values
        input_uids[2] -- climate normal of monthly total precipitation
        input_uids[3] -- climate normal of monthly mean air temperature
        input_uids[4] -- standard deviation of monthly total precipitation from climate normal
        input_uids[5] -- standard deviation of monthly mean air temperature from climate normal
        input_uids[6] -- module parameters:
            Mode -- string, allowed values:
                'segment' -- for each segment
                'data' -- mean over all segments
            Threshold -- none
    
    Output arguments:
        output_uids[0] -- Ped's index, data array of size:
            [segments, lats, lons] -- for Mode == 'segment'
            [lats, lons] -- for Mode == 'data'
"""

from copy import deepcopy

import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import kelvin_to_celsius
from core.mod.calc.calc import Calc

#MAX_N_INPUT_ARGUMENTS = 7
#INPUT_PARAMETERS_INDEX = 6 
PRCP_DATA_UID = 0
TEMP_DATA_UID = 1
PRCP_DATA_NORMALS_UID = 2
TEMP_DATA_NORMALS_UID = 3
PRCP_DATA_STD_UID = 4
TEMP_DATA_STD_UID = 5
DEFAULT_VALUES = {'HTC': 'Selyaninov', 'Mode': 'data', 'Threshold': 10}

class CalcHTC(Calc):
    """ Provides calculation of a spatial field of the Selyaninov's hydrothermal coefficient values or Ped's index values for time series of data.

    """

    def __init__(self, data_helper: DataAccess):
        super().__init__()
        self._data_helper = data_helper

    def _calc_half_htc(self, prcp_values, temp_values, threshold, start, end):
        """ Calculates Selyaninov's hydrothermal coeffcient for a given time period.
        Arguments:
            prcp_values -- daily total precipitation values
            temp_values -- daily mean temperature values
            threshold -- threshold temperature (10C, by default)
            start -- first index in the time grid, we start analysis from it (inclusive)
            end -- last index in the time grid, we stop analysis before it (NOT inclusive)
            If start <= stop algorithm runs forth in time. Runs back, otherwise.
        Returns:
            result -- array of Selyaninov's hydrothermal coefficient values
        """
        # The idea is to subtract threshold from temperature values
        #  and to catch cases when values change sign from negative to positive (a transition)
        #  i.e., pass x-axis upwards.
        # Each crossing corresponds to a possible beginning/ending
        # (depends on the direction of the analysis) of the vegetation period.
        # We sum values between crossings (in segments) and analyse sums according to Ped' (1951).

        # Define a function to detect False->True transition.
        trans = lambda a, b: ma.logical_and(ma.logical_not(a), b)

        # Create shape for new arrays without time dimmension.
        dims = list(temp_values.shape)
        _ = dims.pop(0)

        # Define some useful arrays.
        temp_sums_1 = ma.zeros(dims)  # Temperature sums for the first segment.
        temp_sums_2 = ma.zeros(dims)  # Temperature sums for the second segment.
        temp_sums_3 = ma.zeros(dims)  # Temperature sums for the third segment.
        temp_cnt_1 = ma.zeros(dims)  # Temperature counter for the first segment.
        temp_cnt_2 = ma.zeros(dims)  # Temperature counter for the second segment.
        temp_cnt_3 = ma.zeros(dims)  # Temperature counter for the third segment.
        temp_total = ma.zeros(dims)   # Temperature total sum for a vegetation period.
        prcp_sums_1 = ma.zeros(dims)  # Precipitation sums for the first segment.
        prcp_sums_2 = ma.zeros(dims)  # Precipitation sums for the second segment.
        prcp_sums_3 = ma.zeros(dims)  # Precipitation sums for the third segment.
        prcp_total = ma.zeros(dims)   # Precipitation total sum for a vegetation period.

        trans_mask_1 = ma.zeros(dims)  # Mask of cells in the first segment state
                                      #  (i.e., where the first transition was detected).
        trans_mask_2 = ma.zeros(dims)  # Mask of cells in the second segment state.
        trans_mask_3 = ma.zeros(dims)  # Mask of cells in the third segment state.
        prev_pos_mask = ma.zeros(dims)  # Matrix of previous positive mask state.

        step = 1 if start <= end else -1  # Set step depending on start and stop values.

        # Search for upward transitions and calculate sums.
        for i in range(start, end, step):
            cur_temp = temp_values[i]
            cur_prcp = prcp_values[i]
            temp_deviation = cur_temp - threshold
            temp_pos_mask = temp_deviation >= 0  # Mask of positive values.
            cur_trans = trans(prev_pos_mask, temp_pos_mask)  # Detect negative->positive transition.
            prev_pos_mask = temp_pos_mask  # Store current positive mask for the next iteration.
            # Turn on the third state, if a transition is detected and the second state is on.
            # When the state is on, it is never off (provided by a boolean 'or').
            trans_mask_3 = ma.logical_and(ma.logical_or(cur_trans, trans_mask_3), trans_mask_2)
            # Turn on the second state, if a transition is detected and the first state is on.
            trans_mask_2 = ma.logical_and(ma.logical_or(cur_trans, trans_mask_2), trans_mask_1)
            # Turn on the first state, if a transition is detected or leave it as is.
            trans_mask_1 = ma.logical_or(cur_trans, trans_mask_1)
            # Sum values in the first segment.
            seg_1_mask = ma.logical_and(trans_mask_1, ma.logical_not(trans_mask_2))  # Only for cells in the first segemnt state.
            temp_sums_1[seg_1_mask] += temp_deviation[seg_1_mask]
            temp_cnt_1[seg_1_mask] += 1
            prcp_sums_1[seg_1_mask] += cur_prcp[seg_1_mask]
            # Sum values in the second segment.
            seg_2_mask = ma.logical_and(trans_mask_2, ma.logical_not(trans_mask_3))  # Only for cells in the second segemnt state.
            temp_sums_2[seg_2_mask] += temp_deviation[seg_2_mask]
            temp_cnt_2[seg_2_mask] += 1
            prcp_sums_2[seg_2_mask] += cur_prcp[seg_2_mask]
            # Sum values in the third segment (in fact, everything after the second segment).
            temp_sums_3[trans_mask_3] += temp_deviation[trans_mask_3]
            temp_cnt_3[trans_mask_3] += 1
            prcp_sums_3[trans_mask_3] += cur_prcp[trans_mask_3]

        # Create some intermediate sums.
        temp_sums_12 = temp_sums_1 + temp_sums_2  # S1+S2+S3+S4
        temp_cnt_12 = temp_cnt_1 + temp_cnt_2
        temp_sums_23 = temp_sums_2 + temp_sums_3  # S3+S4+... all the rest
        temp_cnt_23 = temp_cnt_2 + temp_cnt_3
        temp_sums_123 = temp_sums_12 + temp_sums_3  # S1+S2+S3+S4+... all the rest
        temp_cnt_123 = temp_cnt_12 + temp_cnt_3

        # Check Ped's conditions and calculate temperature sums for the vegetation period.
        # By adding temp_cnt_...*threshold we restore original values of the temperature.
        # If vegetation period starts at the first transition point.
        start_at_seg_1 = ma.logical_and(temp_sums_1 >= 0, temp_sums_12 >= 0)  # S1 >= S2 and S1-S2+S3 >= S4
        temp_total[start_at_seg_1] = temp_sums_123[start_at_seg_1] + temp_cnt_123[start_at_seg_1] * threshold
        prcp_total[start_at_seg_1] = prcp_sums_1[start_at_seg_1] + prcp_sums_2[start_at_seg_1] + prcp_sums_3[start_at_seg_1]
        # If vegetation period starts at the second transition point.
        start_at_seg_2 = ma.logical_and(temp_sums_1 < 0, temp_sums_2 >= 0)  # S1 < S2 and S3-S4 >= 0
        temp_total[start_at_seg_2] = temp_sums_23[start_at_seg_2] + temp_cnt_23[start_at_seg_2] * threshold
        prcp_total[start_at_seg_2] = prcp_sums_2[start_at_seg_2] + prcp_sums_3[start_at_seg_2]
        # If vegetation period starts at the third transition point.
        start_at_seg_3 = ma.logical_or(ma.logical_and(temp_sums_1 < 0, temp_sums_2 < 0),  # S1 < S2 and S3 < S4
                                       ma.logical_and(ma.logical_and(temp_sums_1 > 0, temp_sums_2 < 0),  # or
                                                      temp_sums_12 < 0))  # S3 < S4 and S1 > S2 and S1-S2+S3 < S4
        temp_total[start_at_seg_3] = temp_sums_3[start_at_seg_3] + temp_cnt_3[start_at_seg_3] * threshold
        prcp_total[start_at_seg_3] = prcp_sums_3[start_at_seg_3]

        return (prcp_total, temp_total)

    def _calc_htc(self, prcp_values, temp_values, threshold):
        """ Calculates Selyaninov's hydrothermal coeffcient.
        Arguments:
            prcp_values -- daily total precipitation values
            temp_values -- daily mean temperature values
            threshold -- threshold temperature (10C, by default)
        Returns:
            result -- array of Selyaninov's hydrothermal coefficient values
        """
        start = 0
        end = prcp_values.shape[0] - 1
        mid = (end - start) // 2  # Midpoint. Normally should be in the middle of the year. :)
        # Search for the beginning of the vegetation period and sum temperature and precipitation.
        # Run forward, from the beginning of the year to the midpoint.
        prcp_total_1, temp_total_1 = self._calc_half_htc(prcp_values, temp_values, threshold, start, mid+1)
        # Search for the ending of the vegetation period and sum temperature and precipitation.
        # Run backward, from the end of the year to the midpoint.
        prcp_total_2, temp_total_2 = self._calc_half_htc(prcp_values, temp_values, threshold, end, mid)

        # Calculate HTC.
        result = 10 * (prcp_total_1 + prcp_total_2) / (temp_total_1 + temp_total_2)

        return result

    def _calc_ped(self, prcp_values, temp_values, prcp_normals, temp_normals, prcp_std, temp_std):
        """ Calculates Ped's index.
        Arguments:
            prcp_values -- monthly total precipitation values
            temp_values -- monthly mean air temperature values

            prcp_normals -- climate normal of monthly total precipitation
            temp_normals -- climate normal of monthly mean air temperature

            prcp_std -- standard deviation of monthly total precipitation from climate normal
            temp_std -- standard deviation of monthly mean air temperature from climate normal
        Returns:
            result -- array of Ped's index values
        """  
        prcp_delta = prcp_values - prcp_normals
        temp_delta = temp_values - temp_normals

        result = temp_delta / temp_std - prcp_delta / prcp_std
        return result    

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        self.logger.info('Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, 'No input arguments!'

        # Get parameters
        parameters = None
        #if len(input_uids) == MAX_N_INPUT_ARGUMENTS:  # If parameters are given.
        parameters = self._data_helper.get(input_uids[-1])
        calc_htc = self._get_parameter('HTC', parameters, DEFAULT_VALUES)
        calc_mode = self._get_parameter('Mode', parameters, DEFAULT_VALUES)
        if calc_htc == 'Selyaninov':
            threshold = int(self._get_parameter('Threshold', parameters, DEFAULT_VALUES))
            self.logger.info('Threshold: %s', threshold)

        self.logger.info('Hydrothermal coefficient: %s', calc_htc)
        self.logger.info('Calculation mode: %s', calc_mode)
        
        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, 'No output arguments!'

        # Get time segments and levels
        time_segments = self._data_helper.get_segments(input_uids[PRCP_DATA_UID])
        prcp_levels = self._data_helper.get_levels(input_uids[PRCP_DATA_UID])
        temp_levels = self._data_helper.get_levels(input_uids[TEMP_DATA_UID])

        assert len(prcp_levels) == len(temp_levels), \
            'Error! Number of vertical levels are not the same!'

        data_func = ma.mean  # For calc_mode == 'data' we calculate mean over all segments.

        # For calc_htc == 'Ped' we calculate Ped index.
        if calc_htc == 'Ped':
            prcp_normals_levels = self._data_helper.get_levels(input_uids[PRCP_DATA_NORMALS_UID])  # There should be only one level - normals?.
            temp_normals_levels = self._data_helper.get_levels(input_uids[TEMP_DATA_NORMALS_UID])
            assert len(prcp_normals_levels) == len(temp_normals_levels), \
                'Error! Number of vertical levels are not the same!'
            # Normals time segments should be set for year 1 (as set in a normals file)
            normals_time_segments = deepcopy(time_segments) 
            for segment in normals_time_segments:
                segment['@beginning'] = '0001' + segment['@beginning'][4:]
                segment['@ending'] = '0001' + segment['@ending'][4:]
            
            for prcp_level, temp_level, prcp_normals_level, temp_normals_level in zip(prcp_levels, temp_levels, prcp_normals_levels, temp_normals_levels):
                all_segments_values = []
                all_time_grids = []
                for segment, normals_segment in zip(time_segments, normals_time_segments):
                    # Read data
                    prcp_data = self._data_helper.get(input_uids[PRCP_DATA_UID], segments=segment, levels=prcp_level)
                    prcp_values = prcp_data['data'][prcp_level][segment['@name']]['@values']
                    # take this time grid as it coincides with the desired result for calc_mode = 'segment'
                    one_time_grids = prcp_data['data'][prcp_level][segment['@name']]['@time_grid'] 
                    temp_data = self._data_helper.get(input_uids[TEMP_DATA_UID], segments=segment, levels=temp_level)
                    temp_values = temp_data['data'][temp_level][segment['@name']]['@values']

                    # Convert degK to degC if needed
                    if temp_data['data']['description']['@units'] == 'K':
                        temp_values = kelvin_to_celsius(temp_values)

                    # Read monthly precipitation and temperature normals
                    prcp_normals_data = self._data_helper.get(input_uids[PRCP_DATA_NORMALS_UID], segments=normals_segment, levels=prcp_normals_level)
                    prcp_normals = prcp_normals_data['data'][prcp_normals_level][normals_segment['@name']]['@values']
                    temp_normals_data = self._data_helper.get(input_uids[TEMP_DATA_NORMALS_UID], segments=normals_segment, levels=temp_normals_level)
                    temp_normals = temp_normals_data['data'][temp_normals_level][normals_segment['@name']]['@values']
                    
                    # Read monthly precipitation and temperature standard deviation
                    prcp_std_data = self._data_helper.get(input_uids[PRCP_DATA_STD_UID], segments=normals_segment, levels=prcp_normals_level)
                    prcp_std = prcp_std_data['data'][prcp_normals_level][normals_segment['@name']]['@values']
                    temp_std_data = self._data_helper.get(input_uids[TEMP_DATA_STD_UID], segments=normals_segment, levels=temp_normals_level)
                    temp_std = temp_std_data['data'][temp_normals_level][normals_segment['@name']]['@values']

                    # Convert degK to degC if needed
                    if temp_normals_data['data']['description']['@units'] == 'K':
                        temp_normals = kelvin_to_celsius(temp_normals)
                                    
                    # Perform calculation for the current time segment.
                    one_segment_values = self._calc_ped(prcp_values, temp_values, prcp_normals, temp_normals, prcp_std, temp_std)
                    
                    if one_time_grids.shape[0] == 1:
                        one_segment_values = one_segment_values.squeeze(axis = 0)
                        one_time_grid = one_time_grids
                    else:
                        middle_idx = round((len(one_time_grids) - 1) / 2)
                        one_time_grid = [one_time_grids[middle_idx]]
                        one_segment_values = data_func(one_segment_values, axis = 0)

                    # For segment-wise averaging send to the output current time segment results
                    # or store them otherwise.
                    if calc_mode == 'segment':
                        self._data_helper.put(output_uids[0], values=one_segment_values, level=prcp_level, segment=segment, times = one_time_grid,
                                          longitudes=prcp_data['@longitude_grid'],
                                          latitudes=prcp_data['@latitude_grid'],
                                          fill_value=prcp_data['@fill_value'],
                                          meta=prcp_data['meta'])
                    elif calc_mode == 'data':
                        all_segments_values.append(one_segment_values)
                        all_time_grids.append(one_time_grid)
                    else:
                        self.logger.error('Error! Unknown calculation mode: \'%s\'', calc_mode)
                        raise ValueError

                # For data-wise analysis analyse segments analyses :)
                if calc_mode == 'data':
                    values_out = data_func(ma.stack(all_segments_values), axis=0)
                    middle_idx_seg = round((len(all_time_grids) - 1) / 2)
                    all_time_grid = all_time_grids[middle_idx_seg]
                    middle_idx = round((len(all_time_grid) - 1) / 2)
                    result_time_grid = [all_time_grid[middle_idx]]
                                
                    # Make a global segment covering all input time segments
                    full_range_segment = deepcopy(time_segments[0])  # Take the beginning of the first segment...
                    full_range_segment['@ending'] = time_segments[-1]['@ending']  # and the end of the last one.
                    full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

                self._data_helper.put(output_uids[0], values=values_out, level=prcp_level, segment=full_range_segment, times = result_time_grid,
                                      longitudes=prcp_data['@longitude_grid'], latitudes=prcp_data['@latitude_grid'],
                                      fill_value=prcp_data['@fill_value'], meta=prcp_data['meta'])

        # For calc_htc == 'Selyaninov' we calculate Hydrothermal coefficient of Selyaninov.
        else:
            for prcp_level, temp_level in zip(prcp_levels, temp_levels):
                all_segments_values = []
                all_time_grids = []
                for segment in time_segments:
                    # Read data
                    prcp_data = self._data_helper.get(input_uids[PRCP_DATA_UID], segments=segment, levels=prcp_level)
                    prcp_values = prcp_data['data'][prcp_level][segment['@name']]['@values']
                    # take this time grid as it coincides with the desired result for calc_mode = 'segment'
                    one_time_grid = prcp_data['data'][prcp_level][segment['@name']]['@time_grid'] 
                    temp_data = self._data_helper.get(input_uids[TEMP_DATA_UID], segments=segment, levels=temp_level)
                    temp_values = temp_data['data'][temp_level][segment['@name']]['@values']

                    # Convert degK to degC if needed
                    if temp_data['data']['description']['@units'] == 'K':
                        temp_values = kelvin_to_celsius(temp_values)

                    # Perform calculation for the current time segment.
                    one_segment_values = self._calc_htc(prcp_values, temp_values, threshold)                  

                    # For segment-wise averaging send to the output current time segment results
                    # or store them otherwise.
                    if calc_mode == 'segment':
                        self._data_helper.put(output_uids[0], values=one_segment_values, level=prcp_level, segment=segment, times = one_time_grid,
                                          longitudes=prcp_data['@longitude_grid'],
                                          latitudes=prcp_data['@latitude_grid'],
                                          fill_value=prcp_data['@fill_value'],
                                          meta=prcp_data['meta'])
                    elif calc_mode == 'data':
                        all_segments_values.append(one_segment_values)
                        all_time_grids.append(one_time_grid)
                    else:
                        self.logger.error('Error! Unknown calculation mode: \'%s\'', calc_mode)
                        raise ValueError

                # For data-wise analysis analyse segments analyses :)
                if calc_mode == 'data':
                    values_out = data_func(ma.stack(all_segments_values), axis=0)
                    middle_idx_seg = round((len(all_time_grids) - 1) / 2)
                    all_time_grid = all_time_grids[middle_idx_seg]
                    middle_idx = round((len(all_time_grid) - 1) / 2)
                    result_time_grid = [all_time_grid[middle_idx]]
                                
                    # Make a global segment covering all input time segments
                    full_range_segment = deepcopy(time_segments[0])  # Take the beginning of the first segment...
                    full_range_segment['@ending'] = time_segments[-1]['@ending']  # and the end of the last one.
                    full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

                    self._data_helper.put(output_uids[0], values=values_out, level=prcp_level, segment=full_range_segment, times = result_time_grid,
                                      longitudes=prcp_data['@longitude_grid'], latitudes=prcp_data['@latitude_grid'],
                                      fill_value=prcp_data['@fill_value'], meta=prcp_data['meta'])

        self.logger.info('Finished!')

