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

from copy import deepcopy

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

        level_2_name = data_2_add['level']
        segment_2_name = data_2_add['segment']['@name']
        values_2 = data_2['data'][level_2_name][segment_2_name]['@values']
        time_grid_2 = data_2['data'][level_2_name][segment_2_name]['@time_grid']
        lats_2 = data_2['@latitude_grid']
        lons_2 = data_2['@longitude_grid']

        


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
