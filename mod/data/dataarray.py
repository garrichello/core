"""Provides classes
    DataArray
"""

class DataArray:
    """ Provides methods for reading and writing arrays in memory.
    """

    def __init__(self, data_info):
        self._data_info = data_info

        # Create a new levels element in the data info discarding anything specified in the task file.
        self._data_info["data"]["levels"] = {}
        self._data_info["data"]["levels"]["@values"] = set()

        # Create a new time segment element in data info discarding anything specified in the task file.
        self._data_info["data"]["time"] = {}
        self._data_info["data"]["time"]["segment"] = []

    def read(self, segments, levels):
        pass

    def write(self, values, level, segment, times, longitudes, latitudes):
        """ Stores values and metadata in data_info dictionary
            describing 'array' data element.
        """

        if level is not None:
            # Append a new level name.
            self._data_info["data"]["levels"]["@values"].add(level)

        if segment is not None:
            # Append a new time segment.
            try:
                self._data_info["data"]["time"]["segment"].index(segment)
            except ValueError:
                self._data_info["data"]["time"]["segment"].append(segment)

        if times is not None:
            # Append a time grid
            self._data_info["data"]["time"]["@grid"] = times

        self._data_info["data"]["@longitudes"] = longitudes
        self._data_info["data"]["@latitudes"] = latitudes

        if level is not None:
            if self._data_info["data"].get(level) is None:
                self._data_info["data"][level] = {}
            if segment is not None:
                if self._data_info["data"][level].get(segment["@name"]) is None:
                    self._data_info["data"][level][segment["@name"]] = {}
                self._data_info["data"][level][segment["@name"]]["@values"] = values
            else:
                self._data_info["data"][level]["@values"] = values
        else:
           self._data_info["data"]["@values"] = values
       
        pass