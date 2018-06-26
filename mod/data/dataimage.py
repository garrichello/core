"""Provides classes:
    DataImage
"""

class DataImage:
    """ Provides plotting of data to graphical files.
    Supported formats: geoTIFF

    """
    
    def __init__(self, data_info):
        self._data_info = data_info

    def read(self, segments, levels):
        pass    

    def write(self, values, level, segment, times, longitudes, latitudes):
        pass    