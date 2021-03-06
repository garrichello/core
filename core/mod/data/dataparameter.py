"""Provides classes:
    DataParameter
"""
from core.base.common import listify
from .data import Data

class DataParameter(Data):
    """ Provides methods for reading and writing parameters in a task file.
    """

    def __init__(self, data_info):
        super().__init__(data_info)
        self._data_info = data_info

    def _type_cast(self, string_value, cast_type):
        """ Casts string values to the specified type.

        Arguments:
            string_value -- string containig value/values to convert.
            cast_type -- type to cast to.

        Returns:
            result -- scalar or list of scalars of the specified type.
        """

        if cast_type == 'string':
            return string_value
        if cast_type == 'integer':
            return int(string_value)
        if cast_type == 'float':
            return float(string_value)

    def read(self, options):    # pylint: disable=W0613
        """Reads parameters.

        Arguments:
            options -- dictionary of read options:

        Returns:
            result -- dictionary containing parameters from a task file
        """

        self.logger.info('Reading parameters...')

        parameters = listify(self._data_info['data']['param'])
        result = {'@type': 'parameter'}
        for parameter in parameters:
            result[parameter['@uid']] = self._type_cast(parameter['#text'], parameter['@type'])

        self.logger.info('Done')

        return result
