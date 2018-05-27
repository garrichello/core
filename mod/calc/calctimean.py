"""Sample class"""

from base.dataaccess import DataAccess

class cvcCalcTiMean():
    def __init__(self, data_helper: DataAccess):
        self._data_helper = data_helper
    
    def run(self):
        print("(cvcCalcTiMean::run) Started!")

        input_uids = self._data_helper.input_uids()

        time_segments = self._data_helper.get_segments(input_uids[0])

        vertical_levels = self._data_helper.get_levels(input_uids[0])

        for segment in time_segments:
            result = self._data_helper.get(input_uids[0], segment, vertical_levels)

        print("(cvcCalcTiMean::run) Finished!")
        