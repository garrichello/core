""" Class cvcCalcTiMean provides methods for time mean calculation
"""

from copy import copy
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calcmeanmaxmin import CalcMeanMaxMin

MAX_N_INPUT_ARGUMENTS = 2
INPUT_PARAMETERS_INDEX = 1
CALC_MODE = 'timeMean'
DEFAULT_MODE = 'data'

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
