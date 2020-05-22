""" CalcExceedance implements calculation of a spatial field of cold/warm nights/days values for time series of data.

    Input arguments:
        input_uids[0] -- climate normals data:
            minimum daily values -- for Nights
            maximum daily values -- for Days
            set scenario (depends on dataset):
                Climate normals 1961-1990 -- for 1961-1990 base period
                Climate normals 1971-2000 -- for 1971-2000 base period
                Climate normals 1981-2010 -- for 1981-2010 base period
            set parent: UID of study data
            set product: pdftails
            set level:
                10 -- for Cold
                90 -- for Warm
            DO NOT set time segments!
        input_uids[1] -- study data
            minimum daily values -- for Nights
            maximum daily values -- for Days
        input_uids[2] -- module parameters:
            Feature -- string, allowed values:
                'frequency' -- frequency
                'intensity' -- intensity
                'duration'  -- duration
                default value: 'frequency'
            Exceedance -- string, allowed values:
                'low' -- for Cold
                'high' -- for Warm
            Mode -- string, allowed values:
                'segment' -- for each segment
                'data' -- maximum over all segments
    Output arguments:
        output_uids[0] -- Frequency/Intensity/Duration of Cold/Warm Days/Night, data array of size:
            [segments, lats, lons] -- for Mode == 'segment'
            [lats, lons] -- for Mode == 'data'
"""

from copy import deepcopy
import operator
import datetime

import numpy as np
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 3
INPUT_PARAMETERS_INDEX = 2
NORMALS_UID = 0
STUDY_UID = 1
DEFAULT_VALUES = {'Feature': 'frequency', 'Exceedance': 'low', 'Mode': 'data', 'Condition': None}

class CalcExceedance(Calc):
    """ Provides calculation of a spatial field of cold/warm nights/days values for time series of data.

    """

    def __init__(self, data_helper: DataAccess):
        super().__init__()
        self._data_helper = data_helper

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
            try:
                feb29_index = time_list.index(feb29)
                values = np.delete(values, feb29_index, axis=0)
            except ValueError:
                pass

        return values

    def _calc_duration(self, mask):
        """ Calculates a duration of the longest consecutive True values.
        Arguments:
            mask -- mask values for each time step.

        Returns:
            m x n array of the longest consecutive True values.
        """
        counter = ma.zeros(mask.shape[1:])  # Current counter
        duration = ma.zeros(mask.shape[1:])  # Longest sequence
        for array in mask:
            counter += array  # Increment counters in according cells.
            counter *= array  # Reset previously accumulated counts in 0-valued cells.
            idxs = counter > duration
            duration[idxs] = counter[idxs]

        return duration

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        self.logger.info('Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, 'Error! No input arguments!'

        # Get parameters
        parameters = None
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:  # If parameters are given.
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])
        feature = self._get_parameter('Feature', parameters, DEFAULT_VALUES)
        exceedance = self._get_parameter('Exceedance', parameters, DEFAULT_VALUES)
        condition = self._get_parameter('Condition', parameters, DEFAULT_VALUES)
        calc_mode = self._get_parameter('Mode', parameters, DEFAULT_VALUES)

        self.logger.info('Calculation feature: %s', feature)
        self.logger.info('Exceedance: %s', exceedance)
        if condition is not None:
            self.logger.info('Condition: %s', condition)
        self.logger.info('Calculation mode: %s', calc_mode)

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
            self.logger.error('Error! Unknown exceedance value: \'%s\'', exceedance)
            raise ValueError

        data_func = ma.max  # For calc_mode == 'data' we calculate max over all segments.

        input_description = self._data_helper.get_data_info(output_uids[0])['description']
        output_description = {'@title': input_description['@title']}
        if feature != 'total':
            output_description['@title'] = feature.capitalize() + ' of ' + output_description['@title']

        for level in study_vertical_levels:
            all_segments_data = []
            for segment in study_time_segments:
                normals_values = normals_data['data'][percentile][segment['@name']]['@values']
                study_values = study_data['data'][level][segment['@name']]['@values']
                study_time_grid = study_data['data'][level][segment['@name']]['@time_grid']

                # Remove Feb 29 from the study array (we do not take this day into consideration).
                study_values = self._remove_feb29(study_values, study_time_grid)

                # Apply conditions if there are any.
                if condition is not None:
                    self._apply_condition(study_values, condition)

                # Compare values according chosen exceedance.
                comparison_mask = comparison_func(study_values, normals_values)

                # Perform calculation for the current time segment.
                if feature == 'frequency':   # We can just average 'True's to get a fraction and multiply by 100%.
                    one_segment_data = ma.mean(comparison_mask, axis=0) * 100
                    output_description['@units'] = '%'

                if feature == 'intensity':
                    diff = ma.abs(study_values - normals_values)  # Calculate difference
                    diff.mask = ma.mask_or(diff.mask, ~comparison_mask, shrink=False)  # and mask out unnecessary values.
                    one_segment_data = ma.mean(diff, axis=0)
                    output_description['@units'] = 'days'

                if feature == 'duration':
                    one_segment_data = self._calc_duration(comparison_mask)
                    output_description['@units'] = 'days'

                if feature == 'total':
                    study_values.mask = ma.mask_or(study_values.mask, ~comparison_mask, shrink=False)
                    one_segment_data = ma.sum(study_values, axis=0)
                    output_description['@units'] = 'days'

                # For segment-wise averaging send to the output current time segment results
                # or store them otherwise.
                if calc_mode == 'segment':
                    self._data_helper.put(output_uids[0], values=one_segment_data, level=level, segment=segment,
                                          longitudes=study_data['@longitude_grid'],
                                          latitudes=study_data['@latitude_grid'],
                                          fill_value=study_data['@fill_value'],
                                          meta=study_data['meta'], description=output_description)
                elif calc_mode == 'data':
                    all_segments_data.append(one_segment_data)
                else:
                    self.logger.error('Error! Unknown calculation mode: \'%s\'', calc_mode)
                    raise ValueError

            # For data-wise analysis analyse segments analyses :)
            if calc_mode == 'data':
                data_out = data_func(ma.stack(all_segments_data), axis=0)

                # Make a global segment covering all input time segments
                full_range_segment = deepcopy(study_time_segments[0])  # Take the beginning of the first segment...
                full_range_segment['@ending'] = study_time_segments[-1]['@ending']  # and the end of the last one.
                full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

                output_description['@title'] = 'Maximum ' + output_description['@title']

                self._data_helper.put(output_uids[0], values=data_out, level=level, segment=full_range_segment,
                                      longitudes=study_data['@longitude_grid'], latitudes=study_data['@latitude_grid'],
                                      fill_value=study_data['@fill_value'], meta=study_data['meta'], description=output_description)

        self.logger.info('Finished!')
