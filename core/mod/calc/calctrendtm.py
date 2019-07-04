"""Class cvcCalcTrendTM provides methods for trend calculation"""

from core.base.dataaccess import DataAccess

from core.base.common import print

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1

class cvcCalcTrendTM():
    """ Performs calculation of trend of values.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(cvcCalcTrendTM::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(cvcCalcTrendTM::run) No input arguments!'

        # Get parameters
        if len(input_uids) == MAX_N_INPUT_ARGUMENTS:
            parameters = self._data_helper.get(input_uids[INPUT_PARAMETERS_INDEX])

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(cvcCalcTrendTM::run) No output arguments!'

        # Get data for all time segments and levels at once
        time_segments = self._data_helper.get_segments(input_uids[0])
        vertical_levels = self._data_helper.get_levels(input_uids[0])
        result = self._data_helper.get(input_uids[0], segments=time_segments, levels=vertical_levels)

        for level in vertical_levels:
            for segment in time_segments:

                trend_values = None
                self._data_helper.put(output_uids[0], values=trend_values, level=level, segment=segment,
                                      longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'],
                                      fill_value=result['@fill_value'], meta=result['meta'])

        print('(cvcCalcTrendTM::run) Finished!')
