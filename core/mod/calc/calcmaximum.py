""" Class cvcCalcMaximum provides methods for calculation of maximum over time values
"""

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calcbasicstat import CalcBasicStat

CALC_MODE = 'timeMax'

class cvcCalcMaximum(CalcBasicStat):
    """ Performs calculation of time averaged values.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper
        super().__init__(data_helper)

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(cvcCalcMaximum::run) Started!')

        self._run(CALC_MODE)

        print('(cvcCalcMaximum::run) Finished!')
