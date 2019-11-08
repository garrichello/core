""" Class cvcCalcMinimum provides methods for calculation of minimum over time values

    Input arguments:
        input_uids[0] -- data of input values
        input_uids[1] -- module parameters:
            timeMin -- string, allowed values:
                'day' -- daily minimum
                'segment' -- segment minimum
                'data'  -- whole data minimum
                default value: 'data'
    Output arguments:
        output_uids[0] -- minimum values, data array of size:
            [days, lats, lons] -- if timeMin == 'day'
            [segments, lats, lons] -- if timeMin == 'segment'
            [lat, lons] -- if timeMin == 'data'
"""

from core.base.dataaccess import DataAccess
from core.mod.calc.calcbasicstat import CalcBasicStat

CALC_MODE = 'timeMin'

class cvcCalcMinimum(CalcBasicStat):
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
