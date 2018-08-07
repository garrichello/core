"""Sample class"""

from base.dataaccess import DataAccess

class cvcCalcTiMean():
    """ Performs calculation of time averaged values.

    """
    
    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper
    
    def run(self):
        print('(cvcCalcTiMean::run) Started!')

        input_uids = self._data_helper.input_uids()

        time_segments = self._data_helper.get_segments(input_uids[0])

        vertical_levels = self._data_helper.get_levels(input_uids[0])

        # Get data for all time segments and levels at once
        result = self._data_helper.get(input_uids[0], time_segments, vertical_levels)

        output_uids = self._data_helper.output_uids()

        for level in vertical_levels:
            for segment in time_segments:
                # Let's calulate time averaged values
                timean = result['data'][level][segment['@name']]['@values'].mean(axis=0)
                self._data_helper.put(output_uids[0], values=timean, level=level, segment=segment,
                    longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'])
        
        print('(cvcCalcTiMean::run) Finished!')
