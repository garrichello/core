""" Class cvcCalcTiMean_prec provides methods for time mean calculation
    For precipitations.

    Input arguments:
        input_uids[0] -- data of input values
        input_uids[1] -- module parameters:
            timeMeanPrec -- string, allowed values:
                'day' -- daily precipitation sum
                'segment' -- segment precipitation sum
                'data'  -- segment precipitation sum and whole data mean
                default value: 'data'
    Output arguments:
        output_uids[0] -- average values, data array of size:
            [days, lats, lons] -- if timeMeanPrec == 'day'
            [segments, lats, lons] -- if timeMeanPrec == 'segment'
            [lat, lons] -- if timeMeanPrec == 'data'
"""

from core.base.dataaccess import DataAccess
from core.mod.calc.calcbasicstat import CalcBasicStat

CALC_MODE = 'timeMeanPrec'

class cvcCalcTiMean_prec(CalcBasicStat):
    """ Performs calculation of time averaged values.

    """

    def __init__(self, data_helper: DataAccess):
        super().__init__(data_helper)
        self._data_helper = data_helper

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        self.logger.info('Started!')

        self._run(CALC_MODE)

        self.logger.info('Finished!')
