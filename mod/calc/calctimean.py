"""Sample class"""

from base.dataaccess import DataAccess

from base.common import listify, print

class cvcCalcTiMean():
    """ Performs calculation of time averaged values.

    """
    
    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper
    
    def run(self):
        print('(cvcCalcTiMean::run) Started!')

        input_uids = self._data_helper.input_uids()

        time_segments = listify(self._data_helper.get_segments(input_uids[0]))

        vertical_levels = listify(self._data_helper.get_levels(input_uids[0]))

        # Get data for all time segments and levels at once
        result = self._data_helper.get(input_uids[0], levels=vertical_levels[0])
        result = self._data_helper.get(input_uids[0], segments=time_segments[0])
        result = self._data_helper.get(input_uids[0])        

        output_uids = self._data_helper.output_uids()

        for level in vertical_levels:
            for segment in time_segments:
                # Let's calulate time averaged values
                timean = result['data'][level][segment['@name']]['@values'].mean(axis=0)
                self._data_helper.put(output_uids[0], values=timean, level=level, segment=segment,
                    longitudes=result['@longitude_grid'], latitudes=result['@latitude_grid'], 
                    fill_value = result['data'][level][segment['@name']]['@values'].fill_value)
        
        print('(cvcCalcTiMean::run) Finished!')
