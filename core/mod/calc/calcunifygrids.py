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

import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
DATA_1_UID = 0
DATA_2_UID = 1

class CalcUnifyGrids(Calc):
    """ Provides frids unification for two datasets.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def _unify_time_grid(self, values, original_time_grid, target_time_grid, mode):
        """ Transforms data to a given time grid: from fine to coarse.
        Normally data are averaged but some (total precipitation) are summed.
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
            print('(CalcUnifyGrids::_unify_time_grid) Error! \
                Original_time_grid must be finer (contain more points) than target_time_grid!')
            raise ValueError

        if mode == 'mean':
            acc_func = ma.mean  # Average original grid values between target grid points
        elif mode == 'sum':
            acc_func = ma.sum  # Sum original grid values between target grid points
        else:
            print('(CalcUnifyGrids::_unify_time_grid) Error! Unknown time grid harmonization mode: {}. Aborting!'.format(mode))
            raise ValueError

        # Create the result array.
        dims = list(values.shape)
        dims[0] = len(target_time_grid)
        result = ma.zeros(dims)

        # Reduce values over the time dimension.
        store = []  # Stores values inside one target time grid step.
        j = 0  # Runs along target_time_grid.
        # If target grid has daily steps or longer we deal with averaged/total values.
        # So we need to average or sum original grid values for each target grid step:
        #  result(15.05.2001) = acc_func(values(15.05.2001, 00:00), values(15.05.2001, 06:00),
        #                                values(15.05.2001, 12:00), values(15.05.2001, 18:00)).
        if (target_time_grid[1]-target_time_grid[0]).days >= 1:
            for i, cur_values in enumerate(values):  # i runs along original_time_grid.
                if original_time_grid[i] >= target_time_grid[j] and \
                    original_time_grid[i] < target_time_grid[j+1] if j < len(target_time_grid)-1 else True:  # Control right bound.
                    store.append(cur_values)  # Collect values inside one day/month/year
                else:
                    result[j] = acc_func(ma.stack(store), axis=0)  # Apply an appropriate aggregating function.
                    store = []
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
                for i, cur_values in enumerate(values):  # i runs along original_time_grid.
                    if original_time_grid[i] <= target_time_grid[j] and \
                        original_time_grid[i] > target_time_grid[j-1] if j > 0 else True:  # Control left bound.
                        store.append(cur_values)  # Collect values inside one day/month/year
                    else:
                        result[j] = ma.sum(ma.stack(store), axis=0)  # Sum accumulated values.
                        store = []
                        j += 1
            else:
                # ToDo: May be someday there will be interpolation. But for now: only search for exact match.
                for i, cur_values in enumerate(values):  # i runs along original_time_grid.
                    if original_time_grid[i] == target_time_grid[j]:
                        result[j] = cur_values
                        j += 1
                    if j == len(target_time_grid):
                        break
        return result

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
        level_1_name = data_1_add['level']
        segment_1_name = data_1_add['segment']['@name']
        values_1 = data_1['data'][level_1_name][segment_1_name]['@values']
        time_grid_1 = data_1['data'][level_1_name][segment_1_name]['@time_grid']
        lats_1 = data_1['@latitude_grid']
        lons_1 = data_1['@longitude_grid']
        mode_1 = data_1['data']['description']['@acc_mode']

        level_2_name = data_2_add['level']
        segment_2_name = data_2_add['segment']['@name']
        values_2 = data_2['data'][level_2_name][segment_2_name]['@values']
        time_grid_2 = data_2['data'][level_2_name][segment_2_name]['@time_grid']
        lats_2 = data_2['@latitude_grid']
        lons_2 = data_2['@longitude_grid']
        mode_2 = data_2['data']['description']['@acc_mode']

        # Find out which time grid is finer than another.
        # Since time range must be the same for both grids, longer grid is finer.
        if len(time_grid_1) > len(time_grid_2):
            values_1 = self._unify_time_grid(values_1, time_grid_1, time_grid_2, mode_1)
        elif len(time_grid_1) < len(time_grid_2):
            values_2 = self._unify_time_grid(values_2, time_grid_2, time_grid_1, mode_2)


        return (data_1, data_2)

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(CalcUnifyGrids::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(CalcUnifyGrids::run) No input arguments!'

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcUnifyGrids::run) No output arguments!'

        # Get time segments and levels for both datasets
        time_segments_1 = self._data_helper.get_segments(input_uids[DATA_1_UID])
        time_segments_2 = self._data_helper.get_segments(input_uids[DATA_2_UID])
        assert len(time_segments_1) == len(time_segments_2), \
            '(CalcUnifyGrids::run) Error! Number of time segments are not the same!'
        levels_1 = self._data_helper.get_levels(input_uids[DATA_1_UID])
        levels_2 = self._data_helper.get_levels(input_uids[DATA_2_UID])
        assert len(levels_1) == len(levels_2), \
            '(CalcUnifyGrids::run) Error! Number of vertical levels are not the same!'

        for data_1_level, data_2_level in zip(levels_1, levels_2):
            for data_1_segment, data_2_segment in zip(time_segments_1, time_segments_2):
                # Read data
                data_1 = self._data_helper.get(input_uids[DATA_1_UID], segments=data_1_segment, levels=data_1_level)
                data_2 = self._data_helper.get(input_uids[DATA_2_UID], segments=data_2_segment, levels=data_2_level)
                data_1_add = {'level': data_1_level, 'segment': data_1_segment}
                data_2_add = {'level': data_2_level, 'segment': data_2_segment}

                # Perform calculation for the current time segment.
                unidata_1, unidata_2 = self._unify_grids(data_1, data_1_add, data_2, data_2_add)

                self._data_helper.put(output_uids[DATA_1_UID], values=unidata_1['@values'],
                                      level=data_1_level, segment=data_1_segment,
                                      longitudes=unidata_1['@longitude_grid'],
                                      latitudes=unidata_1['@latitude_grid'],
                                      fill_value=unidata_1['@fill_value'],
                                      meta=unidata_1['meta'])

                self._data_helper.put(output_uids[DATA_2_UID], values=unidata_2['@values'],
                                      level=data_2_level, segment=data_2_segment,
                                      longitudes=unidata_2['@longitude_grid'],
                                      latitudes=unidata_2['@latitude_grid'],
                                      fill_value=unidata_2['@fill_value'],
                                      meta=unidata_2['meta'])

        print('(CalcUnifyGrids::run) Finished!')
