"""Sample class"""

class cvcCalcTiMean():
    def __init__(self, data_helper):
        self._data_helper = data_helper
    
    def run(self):
        print("(cvcCalcTiMean::run) Started!")

        input_uids = self._data_helper.get_uids()

        time_segments = self._data_helper.get_segments(input_uids[0])

        vertical_levels = self._data_helper.get_levels(input_uids[0])

        print("(cvcCalcTiMean::run) Finished!")
        