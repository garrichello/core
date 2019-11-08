""" CalcThresholdDays implements calculation of a spatial field of number of days exceeding/failing given threshold.

    Input arguments:
        input_uids[0] -- study data
            minimum daily values -- for Frost Days and Tropical Nights
            maximum daily values -- for Icing Days and Summer Days
        input_uids[1] -- module parameters:
            Condition -- string, allowed values:
                'less' -- for Frost Days and Icing Days
                'greater' -- for Supper Days and Tropical Nights
            Threshold -- float, value that must not be exceeded or reached, same unita as in study data
            Mode -- string, allowed values:
                'segment' -- for each segment
                'data' -- maximum over all segments
    Output arguments:
        output_uids[0] -- number of Frost/Icing/Summer Days or Tropical Nights, data array of size:
            [segments, lats, lons] -- for Mode == 'segment'
            [lats, lons] -- for Mode == 'data'
"""

import operator

from copy import deepcopy
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1
STUDY_UID = 0
DEFAULT_VALUES = {'Condition': 'less', 'Threshold': 273.15, 'Mode': 'data'}

class CalcThresholdDays(Calc):
    """ Provides calculation of a spatial field of cold/warm nights/days values for time series of data.

    """

    def __init__(self, data_helper: DataAccess):
        super().__init__()
        self._data_helper = data_helper

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
        condition = self._get_parameter('Condition', parameters, DEFAULT_VALUES)
        threshold = self._get_parameter('Threshold', parameters, DEFAULT_VALUES)
        calc_mode = self._get_parameter('Mode', parameters, DEFAULT_VALUES)

        self.logger.info('Calculation condition: %s', condition)
        self.logger.info('Threshold: %s', threshold)
        self.logger.info('Calculation mode: %s', calc_mode)

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, 'Error! No output arguments!'

        # Get time segments and levels
        study_time_segments = self._data_helper.get_segments(input_uids[STUDY_UID])
        study_vertical_levels = self._data_helper.get_levels(input_uids[STUDY_UID])

        if condition == 'less':
            comparison_func = operator.lt
        elif condition == 'greater':
            comparison_func = operator.gt
        else:
            self.logger.error('Error! Unknown condition value: \'%s\'', condition)
            raise ValueError

        data_func = ma.max  # For calc_mode == 'data' we calculate max over all segments.

        for level in study_vertical_levels:
            all_segments_data = []
            for segment in study_time_segments:
                # Read data
                study_data = self._data_helper.get(input_uids[STUDY_UID], segments=segment, levels=level)
                study_values = study_data['data'][level][segment['@name']]['@values']

                # Compare values according chosen exceedance.
                comparison_mask = comparison_func(study_values, threshold)

                # Perform calculation for the current time segment.
                one_segment_data = ma.sum(comparison_mask, axis=0)

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
                    self.logger.error('Error! Unknown calculation mode: \'%s\'', calc_mode)
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

        self.logger.info('Finished!')
