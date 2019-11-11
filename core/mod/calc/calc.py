""" Base class cvcCalc provides methods for all calculation classes
"""

import logging
from copy import deepcopy
import numpy.ma as ma

class Calc():
    """ Base class for all calculation methods
    """
    def __init__(self):
        self.logger = logging.getLogger()

    def make_global_segment(self, time_segments):
        """ Makes a global segment covering all input time segments

        Arguments:
            time_segments -- a list of time segments

        """

        full_range_segment = deepcopy(time_segments[0])  # Take the beginning of the first segment...
        full_range_segment['@ending'] = time_segments[-1]['@ending']  # and the end of the last one.
        full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

        return full_range_segment

    def _get_parameter(self, parameter_name, parameters, default_values):
        """ Extracts parameter value by name from a dictionary or returns a default value.
        Arguments:
            parameter_name -- name of the parameter
            parameters -- dictionary of parameters
            default_values -- dictionary of default values of parameters

        Returns: value from a parameters or default value
        """
        value = None
        if parameters is not None:
            value = parameters.get(parameter_name)
        if value is None:
            value = default_values[parameter_name]

        return value

    def _calc_daily_sum(self, one_day_group):
        daily_sum = None
        for one_step_data, _ in one_day_group:  # Iterate over each time step in each group.
            if daily_sum is None:
                daily_sum = deepcopy(one_step_data)
            else:
                daily_sum += one_step_data  # Calculate daily sums.

        return daily_sum

    def _apply_condition(self, data, condition):
        if condition == 'wet':  # Precipitation >=1 mm per day is a condition for a wet day
            mask = data < 1
            data.mask = ma.mask_or(data.mask, mask, shrink=False)
