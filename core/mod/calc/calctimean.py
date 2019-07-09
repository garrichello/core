""" Class cvcCalcTiMean provides methods for time mean calculation
"""

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calcmeanmaxmin import CalcMeanMaxMin

CALC_MODE = 'timeMean'

class cvcCalcTiMean(CalcMeanMaxMin):
    """ Performs calculation of time averaged values.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper
        super().__init__(data_helper)

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(cvcCalcTiMean::run) Started!')

        self._run_common(CALC_MODE)

        print('(cvcCalcTiMean::run) Finished!')
