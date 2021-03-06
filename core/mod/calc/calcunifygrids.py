""" CalcUnifyGrids harmonize grids of two datasets.
Fine grid is interpolated to a coarse one. Reanalysis grid values are interpolated to weather stations coordinates.
Spatial borders should be the same for both datasets. Level lists should be the same also. Time ranges may differ.

    Input arguments:
        input_uids[0] -- first dataset
        input_uids[1] -- second dataset
    Output arguments:
        output_uids[0] -- first dataset and
        output_uids[1] -- second dataset
         at the same spatial and time grids.
"""
from copy import copy, deepcopy
import numpy as np
import numpy.ma as ma
from scipy.interpolate import RegularGridInterpolator

from core.base.dataaccess import DataAccess
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
DATA_1_UID = 0
DATA_2_UID = 1

class CalcUnifyGrids(Calc):
    """ Provides frids unification for two datasets.

    """

    def __init__(self, data_helper: DataAccess):
        super().__init__()
        self._data_helper = data_helper

    def _unify_time_grid(self, values, original_time_grid, target_time_grid, mode):
        """ Transforms values to a given time grid: from fine to coarse.
        Normally data are averaged but some (e.g., total precipitation) are summed.
        Arguments:
            values -- data values (e.g., [time, lat, lon])
            original_time_grid -- original time grid (must be finer than target)
            target_time_grid -- target time grid (must be coarser than original)
            mode -- action mode:
                'mean' -- data are averaged
                'sum' -- data are summed
        Returns:
            result -- transformed along time axis data values
        """
        if len(original_time_grid) < len(target_time_grid):
            self.logger.error('Error! Original_time_grid must be finer (contain more points) than target_time_grid!')
            raise ValueError
        elif len(original_time_grid) == len(target_time_grid):
            # Since this method doesn't provide interpolation yet, if lengths of grids are equal,
            #  we suppose them to be equal and return values unchanged.
            # ToDo: Interpolation should be implemented someday.
            return values

        if mode == 'mean':
            acc_func = ma.mean  # Average original grid values between target grid points
        elif mode == 'sum':
            acc_func = ma.sum  # Sum original grid values between target grid points
        else:
            self.logger.error('Error! Unknown time grid harmonization mode: %s. Aborting!', mode)
            raise ValueError

        # Create the result array.
        dims = list(values.shape)
        dims[0] = len(target_time_grid)
        result = ma.zeros(dims)

        # Reduce values over the time dimension.
        store = []  # Stores values inside one target time grid step.
        i = 0  # Runs along original_time_grid.
        j = 0  # Runs along target_time_grid.
        # If target grid has daily steps or longer we deal with averaged/total values.
        # So we need to average or sum original grid values for each target grid step:
        #  result(15.05.2001) = acc_func(values(15.05.2001, 00:00), values(15.05.2001, 06:00),
        #                                values(15.05.2001, 12:00), values(15.05.2001, 18:00)).
        delta = original_time_grid[0]-target_time_grid[0]  # Time grids relative delta.
        if (target_time_grid[1]-target_time_grid[0]).days >= 1:
            while i < len(values) and j < len(result):
                if original_time_grid[i]-delta >= target_time_grid[j] and \
                    original_time_grid[i]-delta < target_time_grid[j+1] if j < len(target_time_grid)-1 else True:  # Control right bound.
                    store.append(values[i])  # Collect values inside one day/month/year
                    i += 1
                else:
                    result[j] = acc_func(ma.stack(store), axis=0)  # Apply an appropriate aggregating function.
                    store = []
                    j += 1
            if j < len(result):
                result[j] = acc_func(ma.stack(store), axis=0)  # Apply an appropriate aggregating function.
                j += 1
        # If target grid has n-hourly steps we deal with another time grid for instantenous or accumulated values.
        # So we need to interpolate original grid instanteneous values to the target grid:
        #  result(15.05.2001, 06:00) = interpolate(values(15.05.2001, 03:00), values(15.05.2001, 09:00))
        #   or take values only at required steps:
        #  values(00:00), [drop values(03:00)], values(06:00), [drop values(09:00)], values(12:00),...
        # OR we need to accumulate values from previous steps:
        #  result(15.05.2001, 06:00) = values(15.05.2001, 03:00) + values(15.05.2001, 06:00),
        #  result(15.05.2001, 12:00) = values(15.05.2001, 09:00) + values(15.05.2001, 12:00)...
        else:
            if mode == 'sum':  # Accumulate accumulated values :)
                while i < len(values) and j < len(result):
                    if original_time_grid[i]-delta <= target_time_grid[j] and \
                        original_time_grid[i]-delta > target_time_grid[j-1] if j > 0 else True:  # Control left bound.
                        store.append(values[i])  # Collect values inside one day/month/year
                        i += 1
                    else:
                        result[j] = ma.sum(ma.stack(store), axis=0)  # Sum accumulated values.
                        store = []
                        j += 1
                if j < len(result):
                    result[j] = acc_func(ma.stack(store), axis=0)  # Apply an appropriate aggregating function.
                    j += 1
            else:
                # ToDo: Someday there will be interpolation. But for now: only search for the exact match.
                for i, cur_values in enumerate(values):  # i runs along original_time_grid.
                    if original_time_grid[i]-delta == target_time_grid[j]:
                        result[j] = cur_values
                        j += 1
                    if j == len(result):
                        break

        assert (i == len(values) and j == len(result)), 'Error! Time grids lengths are not equal!'

        return result

    def _unify_spatial_grid(self, values, original_grids, target_grids, out_ndim):
        """ Transforms values to a given spatial grid: from fine to coarse.
        Data are 2-D interpolated at each time step.
        Arguments:
            values -- data values (e.g., [time, lat, lon])
            original_grids -- original spatial grids (must be finer than target)
              [0] -- latitudes
              [1] -- longitudes
            target_grids -- target spatial grids (must be coarser than original)
              [0] -- longitudes
              [1] -- latitudes
            out_ndim -- number of dimensions in result array:
              3 -- for reanalysis
              2 -- for weather stations
        Returns:
            result -- data values interpolated to target_grids
        """
        # If values are gridded data, count = n_lon * n_lat.
        # If values are station data, count = n_stations = n_lon = n_lat
        original_lats = original_grids[0]
        original_lons = original_grids[1]
        target_lats = target_grids[0]
        target_lons = target_grids[1]
        n_original_points = len(original_lons) * len(original_lats) if values.ndim > 2 else len(original_lons)
        n_target_points = len(target_lons) * len(target_lats) if values.ndim > 2 else len(target_lons)

        if n_original_points < n_target_points and out_ndim == 3:
            self.logger.error('Error! Original_grid must be finer (contain more points) than target_grid!')
            raise ValueError
        elif n_original_points == n_target_points and out_ndim == 3:
            # If lengths are equal, and ranges are equal (prerequisite), and grids are regular (MUST!)
            #  then spatial grids are equal and we don't need to do anything.
            return values

        # OK, we have three options:
        #  1) reanalysis (values.ndim == 3) -> reanalysis (out_ndim == 3);
        #  2) reanalysis (values.ndim == 3) -> stations (out_ndim == 2);
        #  3) stations (values.ndim == 2) -> stations (out_ndim == 2).

        # 1. Reanalysis -> Reanalysis
        if values.ndim == 3 and out_ndim == 3:
            dims = list(values.shape)
            result = ma.zeros((dims[0], len(target_lats), len(target_lons)))
            target_lats2d, target_lons2d = np.meshgrid(target_lats, target_lons, indexing='ij')
            for i, cur_values in enumerate(values):
                cur_values_nan = cur_values.astype(float).filled(np.nan)
                interp_func = RegularGridInterpolator((original_lats, original_lons), cur_values_nan, bounds_error=False)
                result[i] = interp_func((target_lats2d, target_lons2d))
                result.mask = ma.mask_or(result.mask, np.isnan(result[i]))

        # 2. Reanalysis -> Stations
        if values.ndim == 3 and out_ndim == 2:
            dims = list(values.shape)
            result = ma.zeros((dims[0], len(target_lons)))  # n_lons == n_stations
            for i, cur_values in enumerate(values):
                cur_values_nan = cur_values.filled(np.nan)
                interp_func = RegularGridInterpolator((original_lats, original_lons), cur_values_nan, bounds_error=False)
                result[i] = interp_func((target_lats, target_lons))
                result.mask = ma.mask_or(result.mask, np.isnan(result[i]))

        # 3. Stations -> Stations
        if values.ndim == 2 and out_ndim == 2:
            self.logger.error('Spatial interpolation stations->stations is not implemented yet!')
            raise Exception

#        result.fill_value = values.fill_value
        return result

    def _get_grids(self, data, data_add):
        """ Extracts grids and values from dataset structure.
        Arguments:
            data -- dataset
            data_add -- time segment and level name
        Returns:
            values, time_grid, lats, lons, acc_mode -- grids and values, and data accumulation mode
        """

        level_name = data_add['level']
        segment_name = data_add['segment']['@name']
        values = deepcopy(data['data'][level_name][segment_name]['@values'])
        time_grid = data['data'][level_name][segment_name]['@time_grid']
        lats = data['@latitude_grid']
        lons = data['@longitude_grid']
        acc_mode = data['data']['description']['@acc_mode']
        # If latitudes are given in descending order, revert it. And values also.
        # This is the requirement of the interpolation routine.
        # Needed only for gridded (reanalysis) data.
        if len(lats) > 1 and values.ndim == 3:
            if lats[1] < lats[0]:
                lats = lats[::-1]
                values = values[:, ::-1, :]
            elif lats[1] == lats[0]:
                self.logger.error('Error! Latitude grid of dataset %s is not changing!',
                                  data['data']['description']['@title'])
                raise ValueError

        return values, time_grid, lats, lons, acc_mode

    def _unify_grids(self, data_1, data_1_add, data_2, data_2_add):
        """ Unifies spatial and temporal grids. Transform data to conform them.
        Arguments:
            data_1 -- first dataset
            data_1_add -- time segment and level name
            data_2 -- second dataset
            data_2_add -- time segment and level name
        Returns:
            (out_data_1, out_data_2) -- datasets on unified grids
        """

        # Get grids and values.
        values_1, time_grid_1, lats_1, lons_1, mode_1 = self._get_grids(data_1, data_1_add)
        values_2, time_grid_2, lats_2, lons_2, mode_2 = self._get_grids(data_2, data_2_add)

        result = {}  # Result values and grids.

        # Find out which time grid is finer than another.
        # Due to the fact that time ranges must be the same for both grids, longer grid is finer.
        if len(time_grid_1) >= len(time_grid_2):
            values_1 = self._unify_time_grid(values_1, time_grid_1, time_grid_2, mode_1)
            result['@time_grid'] = copy(time_grid_2)
        else:
            values_2 = self._unify_time_grid(values_2, time_grid_2, time_grid_1, mode_2)
            result['@time_grid'] = copy(time_grid_1)

        # Find out which spatial grid is finer than another.
        # If values are gridded data, count = n_lon * n_lat.
        # If values are station data, count = n_stations = n_lon = n_lat
        n_points_1 = len(lons_1) * len(lats_1) if values_1.ndim > 2 else len(lons_1)
        n_points_2 = len(lons_2) * len(lats_2) if values_2.ndim > 2 else len(lons_2)
        # Due to the fact that spatial range must be the same for both grids, longer lat grid is finer.
        # Reanalysis is always interpolated to stations coordinates.
        if values_1.ndim > values_2.ndim or (values_1.ndim == values_2.ndim and n_points_1 >= n_points_2):
            values_1 = self._unify_spatial_grid(values_1, (lats_1, lons_1), (lats_2, lons_2), values_2.ndim)
            result['@longitude_grid'] = copy(lons_2)
            result['@latitude_grid'] = copy(lats_2)
            result['@fill_value'] = values_1.fill_value  # Take fill_value from the result of interpolation.
        elif values_1.ndim < values_2.ndim or (values_1.ndim == values_2.ndim and n_points_1 < n_points_2):
            values_2 = self._unify_spatial_grid(values_2, (lats_2, lons_2), (lats_1, lons_1), values_1.ndim)
            result['@longitude_grid'] = copy(lons_1)
            result['@latitude_grid'] = copy(lats_1)
            result['@fill_value'] = values_2.fill_value  # Take fill_value from the result of interpolation.
        else:
            self.logger.error('How did we get here?!')
            raise ValueError

        result['@values_1'] = values_1
        result['@values_2'] = values_2
        result['meta'] = {**data_1['meta'], **data_2['meta']}  # Combine both 'meta'

        return result

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        self.logger.info('Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, 'Error! No input arguments!'

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, 'Error! No output arguments!'

        # Get time segments and levels for both datasets
        time_segments_1 = self._data_helper.get_segments(input_uids[DATA_1_UID])
        time_segments_2 = self._data_helper.get_segments(input_uids[DATA_2_UID])
        assert len(time_segments_1) == len(time_segments_2), \
            'Error! Number of time segments are not the same!'
        levels_1 = self._data_helper.get_levels(input_uids[DATA_1_UID])
        levels_2 = self._data_helper.get_levels(input_uids[DATA_2_UID])
        assert len(levels_1) == len(levels_2), \
            'Error! Number of vertical levels are not the same!'

        for data_1_level, data_2_level in zip(levels_1, levels_2):
            for data_1_segment, data_2_segment in zip(time_segments_1, time_segments_2):
                # Read data
                data_1 = self._data_helper.get(input_uids[DATA_1_UID], segments=data_1_segment, levels=data_1_level)
                data_2 = self._data_helper.get(input_uids[DATA_2_UID], segments=data_2_segment, levels=data_2_level)
                data_1_add = {'level': data_1_level, 'segment': data_1_segment}
                data_2_add = {'level': data_2_level, 'segment': data_2_segment}

                # Perform calculation for the current time segment.
                unidata = self._unify_grids(data_1, data_1_add, data_2, data_2_add)

                self._data_helper.put(output_uids[DATA_1_UID], values=unidata['@values_1'],
                                      level=data_1_level, segment=data_1_segment,
                                      times=unidata['@time_grid'],
                                      longitudes=unidata['@longitude_grid'],
                                      latitudes=unidata['@latitude_grid'],
                                      fill_value=unidata['@fill_value'],
                                      meta=unidata['meta'])

                self._data_helper.put(output_uids[DATA_2_UID], values=unidata['@values_2'],
                                      level=data_2_level, segment=data_2_segment,
                                      times=unidata['@time_grid'],
                                      longitudes=unidata['@longitude_grid'],
                                      latitudes=unidata['@latitude_grid'],
                                      fill_value=unidata['@fill_value'],
                                      meta=unidata['meta'])

        self.logger.info('Finished!')
