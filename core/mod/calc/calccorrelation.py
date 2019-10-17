""" CalcCorrelation calculates linear correlation coefficient for two datasets.
Spatial bounds, as well as spatial and time resolutions should be the same for both datasets. 
Level list lengths should be the same also. Time ranges may differ.

    Input arguments:
        input_uids[0] -- first dataset
        input_uids[1] -- second dataset
    Output arguments:
        output_uids[0] -- Pearson's correlation coefficient R
        output_uids[1] -- significance
"""
from scipy.stats import t as student_t
import numpy as np
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.base.common import print  # pylint: disable=W0622
from core.mod.calc.calc import Calc

MAX_N_INPUT_ARGUMENTS = 2
DATA_1_UID = 0
DATA_2_UID = 1

class CalcCorrelation(Calc):
    """ Provides frids unification for two datasets.

    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper

    def _calc_correlation(self, values_1, values_2, conf_level=0.95):
        """ Calculates Pearson's correlation coeffcient.
        Arguments:
            values_1 -- first data
            values_2 -- second data
            conf_level -- confidence level
        Returns:
            (corr_coeff, significance) -- correlation coefficient and significance arrays
        """

        # Calculate Pearson's correlatiob coefficient
        values_cov = ma.sum((values_1 - ma.mean(values_1, axis=0)) * (values_2 - ma.mean(values_2, axis=0)), axis=0)
        corr_coef = values_cov / (ma.std(values_1, axis=0) * ma.std(values_2, axis=0))

        # Calculate significance using t-distribution with n-2 degrees of freedom.
        deg_fr = values_1.shape[0] - 2.  # Degrees of freedom.
        t_distr = ma.abs(corr_coef * ma.sqrt(deg_fr / (1. - corr_coef**2)))  # Student's t-distribution.
        prob = 0.5 + conf_level / 2  # Probability for two tails.
        cr_value = student_t.ppf(prob, deg_fr)  # Student's Critical value.
        significance = ma.greater(t_distr, cr_value)

        return corr_coef, significance

    def run(self):
        """ Main method of the class. Reads data arrays, process them and returns results. """

        print('(CalcCorrelation::run) Started!')

        # Get inputs
        input_uids = self._data_helper.input_uids()
        assert input_uids, '(CalcCorrelation::run) No input arguments!'

        # Get outputs
        output_uids = self._data_helper.output_uids()
        assert output_uids, '(CalcCorrelation::run) No output arguments!'

        # Get time segments and levels for both datasets
        time_segments_1 = self._data_helper.get_segments(input_uids[DATA_1_UID])
        time_segments_2 = self._data_helper.get_segments(input_uids[DATA_2_UID])
        assert len(time_segments_1) == len(time_segments_2), \
            '(CalcCorrelation::run) Error! Number of time segments are not the same!'
        levels_1 = self._data_helper.get_levels(input_uids[DATA_1_UID])
        levels_2 = self._data_helper.get_levels(input_uids[DATA_2_UID])
        assert len(levels_1) == len(levels_2), \
            '(CalcCorrelation::run) Error! Number of vertical levels are not the same!'

        for data_1_level, data_2_level in zip(levels_1, levels_2):
            for data_1_segment, data_2_segment in zip(time_segments_1, time_segments_2):
                # Read data
                data_1 = self._data_helper.get(input_uids[DATA_1_UID], segments=data_1_segment, levels=data_1_level)
                data_2 = self._data_helper.get(input_uids[DATA_2_UID], segments=data_2_segment, levels=data_2_level)
                values_1 = data_1['data'][data_1_level][data_1_segment['@name']]['@values']
                values_2 = data_2['data'][data_2_level][data_2_segment['@name']]['@values']
                meta = {**data_1['meta'], **data_2['meta']}  # Combine meta from both datasets.

                # Perform calculation for the current time segment.
                corr_coef, significance = self._calc_correlation(values_1, values_2)

                self._data_helper.put(output_uids[DATA_1_UID], values=corr_coef,
                                      level=data_1_level, segment=data_1_segment,
                                      longitudes=data_1['@longitude_grid'],
                                      latitudes=data_1['@latitude_grid'],
                                      fill_value=data_1['@fill_value'],
                                      meta=meta)

                self._data_helper.put(output_uids[DATA_2_UID], values=significance,
                                      level=data_1_level, segment=data_1_segment,
                                      times=data_1['@time_grid'],
                                      longitudes=data_1['@longitude_grid'],
                                      latitudes=data_1['@latitude_grid'],
                                      fill_value=data_1['@fill_value'],
                                      meta=meta)

        print('(CalcCorrelation::run) Finished!')
