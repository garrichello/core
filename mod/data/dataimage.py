"""Provides classes:
    DataImage, ImageGeotiff
"""
from osgeo import gdal
from osgeo import osr
import numpy as np
from ext import shapefile

from base.common import load_module, print, make_filename
from base import SLDLegend


class DataImage:
    """ Provides reading/writing data from/to graphical files.
    Supported formats: float geoTIFF.

    """

    def __init__(self, data_info):
        image_class_name = 'Image' + data_info['data']['file']['@type'].capitalize()
        image_class = load_module('mod', image_class_name)
        self._image = image_class(data_info)
        self._data_info = data_info

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

        # Write image file
        self._image.write(values, options)

        # Write legend into an SLD-file
        if (self._data_info['data']['graphics']['legend']['@kind'] == 'file' and
                self._data_info['data']['graphics']['legend']['file']['@type'] == 'xml'):
            self._data_info['data']['graphics']['legend']['@data_kind'] = 'station' if values.ndim == 1 else 'raster'
            legend = SLDLegend(self._data_info)
            legend.write(values, options)


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

        # Prepare data array with masked values replaced with a fill value.
        data = np.ma.filled(values, fill_value=values.fill_value)
        longitudes = options['longitudes']
        latitudes = options['latitudes']

        # Check if we have a (0..180,-180..0) grid and swap parts if its true. Negative should be on the left and increasing.
        if longitudes[0] > longitudes[-1]:
            first_negative_latitude_idx = np.where(longitudes < 0)[0][0]  # It's a border between pos and neg longitudes.
            longitudes = np.concatenate((longitudes[first_negative_latitude_idx:], longitudes[:first_negative_latitude_idx]))
            left_part = data[:, first_negative_latitude_idx:]
            right_part = data[:, :first_negative_latitude_idx]
            data = np.hstack((left_part, right_part))

        # Prepare GeoTIFF driver.
        fmt = 'GTiff'
        drv = gdal.GetDriverByName(fmt)
        metadata = drv.GetMetadata()

        # Check if driver supports Create() method.
        if (gdal.DCAP_CREATE not in metadata) or (metadata[gdal.DCAP_CREATE] != 'YES'):
            print('''(ImageGeotiff::write) Error!
                     Driver {} does not support Create() method.
                     Unable to write GeoTIFF.'''.format(fmt))
            raise AssertionError

        # Write image.
        dims = data.shape
        filename = make_filename(self._data_info, options)

        dataset = drv.Create(filename, dims[1], dims[0], 1, gdal.GDT_Float32)
        dataset.GetRasterBand(1).WriteArray(data)

        # Prepare geokeys.
        gtype = 'EPSG:4326'
        cs = osr.GetWellKnownGeogCSAsWKT(gtype)
        dataset.SetProjection(cs)

        gt = [0, 1, 0, 0, 0, 1]  # Default value.

        # Pixel scale.
        limits = {limit['@role']: int(limit['#text']) for limit in self._data_info['data']['projection']['limits']['limit']}
        gt[1] = longitudes[1] - longitudes[0]
        gt[5] = latitudes[1] - latitudes[0]

        # Top left pixel position.
        gt[0] = longitudes[0]
        gt[3] = latitudes[0]

        dataset.SetGeoTransform(gt)

        dataset = None


class ImageShape:
    """ Provides reading/writing data from/to ESRI shapefiles.

    """
    def __init__(self, data_info):
        self._data_info = data_info

    def read(self, options):
        """Reads ESRI shapefile into an array.

        Arguments:
            options -- dictionary of read options
        """

        pass

    def write(self, values, options):
        """Writes data array into a ESRI shapefile.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options:
                ['level'] -- vertical level name
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
                ['description'] -- dictionary describing data:
                    ['title'] -- general title of the data (e.g., Average)
                    ['name'] --  name of the data (e.g., Temperature)
                    ['units'] -- units of th data (e.g., K)
                ['meta'] -- additional metadata
                    ['stations'] -- weather stations metadata
                        ['@names'] -- names of stations
                        ['@wmo_codes'] -- WMO codes of stations
                        ['@elevations'] -- elevations of stations

        """

        filename = make_filename(self._data_info, options)
        with shapefile.Writer(filename) as shape_writer:
            if (values.ndim == 1):  # 1-D values means we have stations without a time dimension
                # Get stations with valid data only
                valid_values = values[~values.mask]
                valid_lon = options['longitudes'][~values.mask]
                valid_lat = options['latitudes'][~values.mask]
                valid_name = options['meta']['stations']['@names'][~values.mask]
                valid_elevation = options['meta']['stations']['@elevations'][~values.mask]
                valid_wmo_code = options['meta']['stations']['@wmo_codes'][~values.mask]

                shape_writer.shapeType = shapefile.POINTZ
                shape_writer.field('VALUE', 'N', decimal=8)
                shape_writer.field('WMO_CODE', 'N', decimal=0)
                shape_writer.field('NAME', 'C')

                for i in range(len(valid_values)):
                    shape_writer.pointz(valid_lon[i], valid_lat[i], z=valid_elevation[i])
                    shape_writer.record(valid_values.data[i], valid_wmo_code[i], valid_name[i])

                shape_writer.close()
