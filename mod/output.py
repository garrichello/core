"""Class for output"""

from base.dataaccess import DataAccess

from base.common import print

class cvcOutput:
    """ Provides redirection of input data arrays to corresponding plotting/writing modules
    
    """

    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper
        
    def run(self):
        print('(cvcOutput::run) Started!')

        input_uids = self._data_helper.input_uids()

        time_segments = self._data_helper.get_segments(input_uids[0])

        vertical_levels = self._data_helper.get_levels(input_uids[0])

        # Get data for all time segments and levels at once
        result = self._data_helper.get(input_uids[0], time_segments, vertical_levels)

        output_uids = self._data_helper.output_uids()

        for level_name in vertical_levels:
            for segment in time_segments:
                self._data_helper.put(output_uids[0], result['data'][level_name][segment['@name']]['@values'], level=level_name, 
                        segment=segment, longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'], 
                        description=result['data'][level_name][segment['@name']]['description'], meta=result['meta'])

        print('(cvcOutput::run) Finished!')
