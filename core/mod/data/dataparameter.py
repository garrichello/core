"""Provides classes:
    DataParameter
"""
from core.base.common import listify, print

class DataParameter:
    """ Provides methods for reading and writing parameters in a task file.
    """

    def __init__(self, data_info):
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


    def read(self, options):    # pylint: disable=W0613
        """Reads parameters.

        Arguments:
            options -- dictionary of read options:

        Returns:
            result -- dictionary containing parameters from a task file
        """

        print(' (DataParameter::read) Reading parameters...')

        parameters = listify(self._data_info['data']['param'])
        result = {}
        for parameter in parameters:
            result[parameter['@uid']] = self._type_cast(parameter['#text'], parameter['@type'])

        print(' (DataParameter::read) Done')

        return result
