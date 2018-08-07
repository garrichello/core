"""Provides classes:
    DataImage, ImageGeotiff
"""
from osgeo import gdal

from base.common import load_module
import numpy as np

class DataImage:
    """ Provides reading/writing data from/to graphical files.
    Supported formats: float geoTIFF.

    """
    
    def __init__(self, data_info):
        image_class_name = 'Image' + data_info['data']['file']['@type'].capitalize()
        image_class = load_module('mod', image_class_name)
        self._image = image_class(data_info)

    def read(self, options):
        """Reads image-file into an array.

        Arguments:
            options -- dictionary of read options

        Returns:
            result['array'] -- data array
        """

        result = self._image.read(options)

        return result

    def write(self, values, options):
        """Writes data (and metadata) to an output image-file.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options

        """    

        self._image.write(values, options)

        
class ImageGeotiff:
    """ Provides reading/writing data from/to Geotiff files.

    """
    def __init__(self, data_info):
        self._data_info = data_info

    def read(self, options):
        """Reads Geotiff file into an array.

        Arguments:
            options -- dictionary of read options
        """

        pass

    def write(self, values, options):
        """Writes data array into a Geotiff file.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options:
                ['level'] -- vertical level name 
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
        """    
        fmt = 'GTiff'
        drv = gdal.GetDriverByName(fmt)
        metadata = drv.GetMetadata()
        if metadata.has_key(gdal.DCAP_CREATE) and metadata[gdal.DCAP_CREATE] == 'YES':
            pass
        else:
            print('(ImageGeotiff::write) Error! Driver {} does not support Create() method. Unable to write GeoTIFF.'.format(fmt))
            raise AssertionError
        dt = gdal.GDT_Float32

