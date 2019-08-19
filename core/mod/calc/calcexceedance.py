""" CalcExceedance implements calculation of a spatial field of cold/warm nights/days values for time series of data.
"""

from copy import deepcopy
import operator
import datetime

import numpy as np
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 3
INPUT_PARAMETERS_INDEX = 2
NORMALS_UID = 0
STUDY_UID = 1
DEFAULT_PARAMETERS = {'Feature': 'frequency', 'Exceedance': 'low', 'Mode': 'data'}

class CalcExceedance(Calc):
    """ Provides calculation of a spatial field of cold/warm nights/days values for time series of data.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def _get_parameter(self, parameter_name, parameters):
        """ Extracts parameter value by name from a dictionary or returns a default value.
        Arguments:
            parameter_name -- name of the parameter
            parameters -- dictionary of parameters

        Returns: value from a parameters or default value
        """
        value = parameters.get(parameter_name)
        if value is None:
            value = DEFAULT_PARAMETERS[parameter_name]

        return value

    def _remove_feb29(self, values, time_grid):
        """ Removes February 29 from data and time grid
        Arguments:
            data -- data values
            time_grid -- time grid
        """
        try:
            feb29 = datetime.datetime(time_grid[0].year, 2, 29, time_grid[0].hour, time_grid[0].minute)
        except ValueError:
            feb29 = None
        if feb29:
            time_list = time_grid.tolist()
            feb29_index = time_list.index(feb29)
            values = np.delete(values, feb29_index, axis=0)

        return values

    def _calc_duration(self, mask):
        """ Calculates a duration of the longest consecutive True values.
        Arguments:
            mask -- mask values for each time step.

        Returns:
            m x n array of the longest consecutive True values.
        """
        counter = ma.zeros(mask.shape)  # Current counter
        duration = ma.zeros(mask.shape)  # Longest sequence
        for array in mask:
            counter += array  # Increment counters in according cells.
            counter *= array  # Reset previously accumulated counts in 0-valued cells.
            idxs = counter > duration
            duration[idxs] = counter[idxs]

        return duration

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(CalcExceedance::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(CalcExceedance::run) No input arguments!'

        # Get parameters
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:  # If parameters are given.
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
            feature = self._get_parameter('Feature', parameters)
            exceedance = self._get_parameter('Exceedance', parameters)
            calc_mode = self._get_parameter('Mode', parameters)

        print('(CalcExceedance::run) Calculation feature: {}'.format(feature))
        print('(CalcExceedance::run) Exceedance: {}'.format(exceedance))
        print('(CalcExceedance::run) Calculation mode: {}'.format(calc_mode))

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcExceedance::run) No output arguments!'

        # Get time segments and levels
        study_time_segments = self._data_helper.get_segments(input_uids[STUDY_UID])
        study_vertical_levels = self._data_helper.get_levels(input_uids[STUDY_UID])

        # Normals time segments should be set for year 1 (as set in a pdftails file)
        normals_time_segments = deepcopy(study_time_segments)
        percentile = self._data_helper.get_levels(input_uids[NORMALS_UID])[0]  # There should be only one level - percentile.
        for segment in normals_time_segments:
            segment['@beginning'] = '0001' + segment['@beginning'][4:]
            segment['@ending'] = '0001' + segment['@ending'][4:]

        # Read normals data
        normals_data = self._data_helper.get(input_uids[NORMALS_UID], segments=normals_time_segments)
        study_data = self._data_helper.get(input_uids[STUDY_UID], segments=study_time_segments)

        if exceedance == 'low':
            comparison_func = operator.lt
        elif exceedance == 'high':
            comparison_func = operator.gt
        else:
            print('(CalcExceedance::run) Error! Unknown exceedance value: \'{}\''.format(exceedance))
            raise ValueError

        data_func = ma.max  # For calc_mode == 'data' we calculate max over all segments.

        for level in study_vertical_levels:
            all_segments_data = []
            for segment in study_time_segments:
                normals_values = normals_data['data'][percentile][segment['@name']]['@values']
                study_values = study_data['data'][level][segment['@name']]['@values']
                study_time_grid = study_data['data'][level][segment['@name']]['@time_grid']

                # Remove Feb 29 from the study array (we do not take this day into consideration).
                study_values = self._remove_feb29(study_values, study_time_grid)

                # Compare values according chosen exceedance.
                comparison_mask = comparison_func(study_values, normals_values)

                # Perform calculation for the current time segment.
                if feature == 'frequency':   # We can just average 'True's to get a fraction and multiply by 100%.
                    one_segment_data = ma.mean(comparison_mask, axis=0) * 100

                if feature == 'intensity':
                    diff = ma.abs(study_values - normals_values)  # Calculate difference
                    diff.mask = ma.mask_or(diff.mask, ~comparison_mask, shrink=False)  # and mask out unnecessary values.
                    one_segment_data = ma.mean(diff, axis=0)

                if feature == 'duration':
                    one_segment_data = self._calc_duration(comparison_mask)

                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if calc_mode == 'segment':
                    self._data_helper.put(output_uids[0], values=one_segment_data, level=level, segment=segment,
                                          longitudes=study_data['@longitude_grid'],
                                          latitudes=study_data['@latitude_grid'],
                                          fill_value=study_data['@fill_value'],
                                          meta=study_data['meta'])
                elif calc_mode == 'data':
                    all_segments_data.append(one_segment_data)
                else:
                    print('(CalcExceedance::run) Error! Unknown calculation mode: \'{}\''.format(calc_mode))
                    raise ValueError

            # For data-wise analysis analyse segments analyses :)
            if calc_mode == 'data':
                data_out = data_func(ma.stack(all_segments_data), axis=0)

                # Make a global segment covering all input time segments
                full_range_segment = deepcopy(study_time_segments[0])  # Take the beginning of the first segment...
                full_range_segment['@ending'] = study_time_segments[-1]['@ending']  # and the end of the last one.
                full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

                self._data_helper.put(output_uids[0], values=data_out, level=level, segment=full_range_segment,
                                      longitudes=study_data['@longitude_grid'], latitudes=study_data['@latitude_grid'],
                                      fill_value=study_data['@fill_value'], meta=study_data['meta'])

        print('(CalcExceedance::run) Finished!')
