""" Base class cvcCalc provides methods for all calculation classes
"""

from copy import copy

class Calc():
    """ Base class for all calculation methods
    """

    def make_global_segment(self, time_segments):
        """ Makes a global segment covering all input time segments

        Arguments:
            time_segments -- a list of time segments

        """

        full_range_segment = copy(time_segments[0])  # Take the beginning of the first segment...
        full_range_segment['@ending'] = time_segments[-1]['@ending']  # and the end of the last one.
        full_range_segment['@name'] = 'GlobalSeg'  # Give it a new name.

        return full_range_segment
