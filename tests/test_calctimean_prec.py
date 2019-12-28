import unittest
from unittest.mock import MagicMock
import datetime
import numpy as np
import numpy.ma as ma

from core.base.dataaccess import DataAccess
from core.mod.calc.calctimean_prec import cvcCalcTiMean_prec

SAMPLE_INPUT = {'@dimensions': ['time', 'latitude', 'longitude'],
                '@fill_value': -32767,
                '@grid_type': 'regular',
                '@latitude_grid': ma.MaskedArray(data=[78.], mask=False),
                '@longitude_grid': ma.MaskedArray(data=[51.], mask=False),
                'data': {'2m': {'Seg1': {'@time_grid': np.array([datetime.datetime(1979, 1, 1, 0, 0),
                                                                 datetime.datetime(1979, 1, 1, 0, 6),
                                                                 datetime.datetime(1979, 1, 1, 0, 12),
                                                                 datetime.datetime(1979, 1, 1, 0, 18)]),
                                         '@values': ma.MaskedArray(data=[[[261.]], [[262.]], [[263.]], [[264.]]],  # 788
                                                                   mask=[[[False]], [[True]], [[False]], [[False]]],
                                                                   fill_value=-32767.0),
                                         'segment': {'@name':      'Seg1',
                                                     '@beginning': '1979010100',
                                                     '@ending':    '1979010123'}},
                                'Seg2': {'@time_grid': np.array([datetime.datetime(1980, 1, 1, 0, 0),
                                                                 datetime.datetime(1980, 1, 1, 0, 6),
                                                                 datetime.datetime(1980, 1, 1, 0, 12),
                                                                 datetime.datetime(1980, 1, 1, 0, 18)]),
                                         '@values': ma.MaskedArray(data=[[[271.]], [[272.]], [[273.]], [[274.]]],  # 1090
                                                                   mask=[[[False]], [[False]], [[False]], [[False]]],
                                                                   fill_value=-32767.0),
                                         'segment': {'@name':      'Seg2',
                                                     '@beginning': '1980010100',
                                                     '@ending':    '1980010123'}}},
                         'description': {'@title': 'ERA-Interim Reanalysis',
                                         '@name': 'Air temperature',
                                         '@units': 'K'}},
                'meta': None}

class TimeMeanTest(unittest.TestCase):

    def _get(self, uid, segments=None, levels=None):
        if uid == 'P1Parameter1':
            result = {'timeMeanPrec':'data'}
        elif uid == 'P1Input1':
            result = SAMPLE_INPUT
        else:
            result = None

        return result

    def _get_segments(self, uid):
        if uid == "P1Input1":
            time_segments = [{'@beginning':'1979010100',
                              '@ending':   '1979010123',
                              '@name':     'Seg1'},
                             {'@beginning':'1980010100',
                              '@ending':   '1980010123',
                              '@name':     'Seg2'}]
        else:
            time_segments = None
        return time_segments

    def _get_levels(self, uid):
        if uid == "P1Input1":
            levels = ['2m']
        else:
            levels = None
        return levels

    def test_can_instantiate_mean_module(self):
        data_helper = DataAccess(None, None, None)
        time_mean_module = cvcCalcTiMean_prec(data_helper)
        self.assertIsInstance(time_mean_module, cvcCalcTiMean_prec)

    def test_one_point_mean_is_correct(self):
        input_uids = ["P1Input1", "P1Parameter1"]
        output_uids = ["P1Output1"]

        data_helper = DataAccess(None, None, None)
        data_helper.input_uids = MagicMock(return_value=input_uids)
        data_helper.output_uids = MagicMock(return_value=output_uids)
        data_helper.get = MagicMock(side_effect=self._get)
        data_helper.get_segments = MagicMock(side_effect=self._get_segments)
        data_helper.get_levels = MagicMock(side_effect=self._get_levels)
        data_helper.put = MagicMock(return_value=None)

        time_mean_module = cvcCalcTiMean_prec(data_helper)
        time_mean_module.run()

        args, kwargs = data_helper.put.call_args_list[0]

        self.assertTrue(all(a == b for a, b in zip(output_uids, args)))
        self.assertEqual(kwargs['values'], [[939]], msg='Got: {}'.format(kwargs['values']))
