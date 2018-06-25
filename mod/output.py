"""Class for output"""

class cvcOutput:
    def __init__(self, data_helper):
        self._data_helper = data_helper
        
    def run(self):
        print("(cvcOutput::run) Started!")

        input_uids = self._data_helper.input_uids()

        time_segments = self._data_helper.get_segments(input_uids[0])

        vertical_levels = self._data_helper.get_levels(input_uids[0])

        # Get data for all time segments and levels at once
        result = self._data_helper.get(input_uids[0], time_segments, vertical_levels)

        output_uids = self._data_helper.output_uids()

        pass