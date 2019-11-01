""" Class cvcCalcTiMean provides methods for time mean calculation

    Input arguments:
        input_uids[0] -- data of input values
        input_uids[1] -- module parameters:
            timeMean -- string, allowed values:
                'day' -- daily mean
                'segment' -- segment mean
                'data'  -- whole data mean
                default value: 'data'
    Output arguments:
        output_uids[0] -- average values, data array of size:
            [days, lats, lons] -- if timeMean == 'day'
            [segments, lats, lons] -- if timeMean == 'segment'
            [lat, lons] -- if timeMean == 'data'
"""

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calcbasicstat import CalcBasicStat

CALC_MODE = 'timeMean'

class cvcCalcTiMean(CalcBasicStat):
    """ Performs calculation of time averaged values.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper
        super().__init__(data_helper)

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(cvcCalcTiMean::run) Started!')

        self._run(CALC_MODE)

        print('(cvcCalcTiMean::run) Finished!')
