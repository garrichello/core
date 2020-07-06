""" CalcHTCPed implements calculation of a spatial field of the Ped drought index [Ped, 1975].
    
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
DEFAULT_VALUES = {'Mode': 'data'}

class CalcHTCPed(Calc):
    """ Provides calculation of a spatial field of the Ped's index values for time series of data.

    """

    def __init__(self, data_helper: DataAccess):
        super().__init__()
        self._data_helper = data_helper

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
        
        calc_mode = self._get_parameter('Mode', parameters, DEFAULT_VALUES)
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
                    one_segment_value = one_segment_values.squeeze(axis = 0)
                    one_time_grid = one_time_grids
                else:
                    middle_idx = round((len(one_time_grids) - 1) / 2)
                    one_time_grid = [one_time_grids[middle_idx]]
                    one_segment_value = data_func(one_segment_values, axis = 0)

                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if calc_mode == 'segment':
                    self._data_helper.put(output_uids[0], values=one_segment_value, level=prcp_level, segment=segment, times = one_time_grid,
                                          longitudes=prcp_data['@longitude_grid'],
                                          latitudes=prcp_data['@latitude_grid'],
                                          fill_value=prcp_data['@fill_value'],
                                          meta=prcp_data['meta'])
                elif calc_mode == 'data':
                    all_segments_values.append(one_segment_value)
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

