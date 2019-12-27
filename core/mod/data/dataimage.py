"""Provides classes:
    DataImage, ImageGeotiff
"""
import logging
from copy import deepcopy
from osgeo import gdal
from osgeo import osr
import numpy as np
from scipy.interpolate import griddata
from scipy.spatial import cKDTree as KDTree
from core.ext import shapefile

from core.base.common import load_module, make_filename, make_raw_filename, listify, unlistify
from core.base import SLDLegend
from .data import Data

class DataImage(Data):
    """ Provides reading/writing data from/to graphical files.
    Supported formats: float geoTIFF.

    """

    def __init__(self, data_info):
        super().__init__(data_info)
        image_class_name = 'Image' + data_info['data']['file']['@type'].capitalize()
        image_class = load_module(self.__module__, image_class_name)
        self._image = image_class(data_info)
        self._data_info = data_info

    def _uniform_grids(self, values, options):
        """Checks and makes geographical grids to be regular.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options

        Returns:
            values_regular, options_regular -- data and options on regular grids.
        """

        self.logger.info('Checking grid uniformity...')
        if values.ndim == 2:  # If it is a grid, check it for uniformity.
            if ((options['longitudes'].ndim == 2) and (options['latitudes'].ndim == 2)):
                lons = np.sort(options['longitudes'][0, :])
                dlons = [lons[i+1] - lons[i] for i in range(len(lons)-1)]
                lats = np.sort(options['latitudes'][:, 0])
                dlats = [lats[i+1] - lats[i] for i in range(len(lats)-1)]
                should_regrid = True
            elif ((options['longitudes'].ndim == 1) and (options['latitudes'].ndim == 1)):
                eps = 1e-10  # Some small value. If longitude of latitudes vary more than eps, the grid is irregular.
                lons = np.sort(options['longitudes'])
                dlons = [lons[i+1] - lons[i] for i in range(len(lons)-1)]
                lats = np.sort(options['latitudes'])
                dlats = [lats[i+1] - lats[i] for i in range(len(lats)-1)]
                if ((np.std(dlons) > eps) or (np.std(dlats) > eps)):
                    should_regrid = True
                else:
                    should_regrid = False
            else:
                self.logger.error('Bad longitude/latitide grid dimensions! Aborting...')
                raise ValueError
        elif values.ndim == 1:
            should_regrid = False
        else:
            self.logger.error('Bad values shape! Aborting...')
            raise ValueError
        self.logger.info('Done!')

        # If the grid is irregular (except the stations case, of course), regrid it to a regular one.
        if should_regrid:
            self.logger.info('Non-uniform grid. Regridding...')
            options_regular = deepcopy(options)

            # Create a uniform grid.
            dlon_regular = np.min(dlons) / 2.0  # Half the step to avoid a strange latitudinal shift.
            dlat_regular = np.min(dlats) / 2.0
            nlons_regular = int(np.ceil((np.max(lons) - np.min(lons)) / dlon_regular + 1))
            nlats_regular = int(np.ceil((np.max(lats) - np.min(lats)) / dlat_regular + 1))
            options_regular['longitudes'] = np.arange(nlons_regular) * dlon_regular + lons[0]
            options_regular['latitudes'] = np.arange(nlats_regular) * dlat_regular + lats[0]

            # Prepare data.
            if options['longitudes'].ndim == 1:
                llon, llat = np.meshgrid(options['longitudes'], options['latitudes'])
            else:
                llon, llat = options['longitudes'], options['latitudes']
            llon_regular, llat_regular = np.meshgrid(options_regular['longitudes'], options_regular['latitudes'])
            # Interpolate.
            interp = griddata((llon.ravel(), llat.ravel()), values.ravel(),
                              (llon_regular.ravel(), llat_regular.ravel()), method='nearest')
            # Mask values outside original area.
            tree = KDTree(np.c_[llon.ravel(), llat.ravel()])
            dist, _ = tree.query(np.c_[llon_regular.ravel(), llat_regular.ravel()], k=1)
            lat_lims = np.asarray([44, 60, 68, 73, 76, 78, 79, 80, 81, 82, 83])  # Magic latitudes.
            i = np.searchsorted(lat_lims, np.max(llat), side='left')
            k = [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.5]  # Magic coefficients.
            interp[dist > k[i]] = values.fill_value
            fill_value_mask = interp == values.fill_value
            # combined_mask = ma.mask_or(fill_value_mask, ROI_mask_time, shrink=False)
            # Reshape.
            values_regular = np.reshape(interp, (nlats_regular, nlons_regular))
            values_regular.fill_value = values.fill_value
            self.logger.info('Done!')
        else:
            values_regular = values
            options_regular = options

        return values_regular, options_regular

    def read(self, options):
        """Reads image-file into an array.

        Arguments:
            options -- dictionary of read options

        Returns:
            result['array'] -- data array
        """

        result = self._image.read(options)

        return result

    def write(self, all_values, all_options_dict):
        """Writes data (and metadata) to an output image-file. Supports values from several UIDs.

        Arguments:
            all_values -- processing result's values as a list of masked array/array/list
            all_options_dict -- dictionary of write options (list of dictionaries in multiband case)

        """

        self.logger.info('Writing image...')

        # Make geographical grids uniform.
        all_values_regular = []
        all_options_regular = []
        # In multiband case all_options_dict is a dictionary of lists. Convert it to a list of dictionaries.
        all_options = self._transpose_dict(all_options_dict)
        for values, options in zip(all_values, listify(all_options)):
            val_reg, opt_reg = self._uniform_grids(values, options)
            all_values_regular.append(val_reg)
            all_options_regular.append(opt_reg)

        # Set data kind to define the kind of the legend (for vector or raster data).
        self._data_info['data']['graphics']['legend']['@data_kind'] = 'station' if all_values_regular[0].ndim == 1 else 'raster'
        legend = SLDLegend(self._data_info)

        write_xml_legend = self._data_info['data']['graphics']['legend']['@kind'] == 'file' and \
                           self._data_info['data']['graphics']['legend']['file']['@type'] == 'xml'

        # Write image file.
        if self._image.multiband_support:  # Image supports multiband write.
            self._image.write(all_values_regular, all_options_regular)

            # Write legend into an SLD-file.
            if write_xml_legend:
                legend.write(all_values_regular, all_options_regular)

        else:  # Image does NOT support multiband write.
            for values_regular, options_regular in zip(all_values_regular, all_options_regular):
                self._image.write(values_regular, options_regular)

                # Write legend into an SLD-file.
                if write_xml_legend:
                    legend.write(values_regular, options_regular)

        self.logger.info('Done!')


class ImageGeotiff():
    """ Provides reading/writing data from/to Geotiff files.

    """
    def __init__(self, data_info):
        self.logger = logging.getLogger()
        self._data_info = data_info
        self.multiband_support = True

    def _prepare_data(self, values, options):
        """Prepares data for writing

        Arguments:
            values -- masked data array to be written
            options -- write options including grids

        Returns:
            data, longitudes, latitudes -- prepared data and geographical grids
        """
        data = np.ma.filled(values, fill_value=values.fill_value)
        longitudes = options['longitudes']
        latitudes = options['latitudes']

        # Check if we have a (0..180,-180..0) grid and swap parts if its true. 
        # Negative should be on the left and increasing.
        if longitudes[0] > longitudes[-1]:
            first_negative_latitude_idx = np.where(longitudes < 0)[0][0]  # It's a border between pos and neg longitudes.
            longitudes = np.concatenate((longitudes[first_negative_latitude_idx:],
                                         longitudes[:first_negative_latitude_idx]))
            left_part = data[:, first_negative_latitude_idx:]
            right_part = data[:, :first_negative_latitude_idx]
            data = np.hstack((left_part, right_part))

        return data, longitudes, latitudes

    def read(self, options):
        """Reads Geotiff file into an array.

        Arguments:
            options -- dictionary of read options
        """


    def write(self, all_values: list, all_options: list):
        """Writes data array into a Geotiff file. Supports multiband write.

        Arguments:
            all_values -- processing result's values as a list of masked arrays/arrays/lists.
            all_options -- list dictionaries of write options:
                ['level'] -- vertical level name
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
        """

        self.logger.info('Writing geoTIFF...')

        # Check if it's a multiband case.
#        multiband_write = True if isinstance(all_values, list) else False

        # Prepare data array with masked values replaced with a fill value.
        all_data = []
        for values, options in zip(all_values, all_options):
            cur_data, longitudes, latitudes = self._prepare_data(values, options)
            all_data.append(cur_data)

        # Prepare GeoTIFF driver.
        fmt = 'GTiff'
        drv = gdal.GetDriverByName(fmt)
        metadata = drv.GetMetadata()

        # Check if driver supports Create() method.
        if (gdal.DCAP_CREATE not in metadata) or (metadata[gdal.DCAP_CREATE] != 'YES'):
            self.logger.error('''Error!
                      Driver %s does not support Create() method.
                      Unable to write GeoTIFF.''', fmt)
            raise AssertionError

        # Prepare file name
        filename = make_raw_filename(self._data_info, all_options)

        # Stack multiband data into 3-D array. Leftmost dimension is a band.
        if all_data[0].ndim == 2:  # 2-D arrays stack to create 3-D data array
            data_to_write = np.stack(all_data)
        elif all_data[0].ndim == 3:  # 3-D arrays vstack to create 3-D data array also.
            data_to_write = np.vstack(all_data)
        else:
            self.logger.error('Incorrect number of dimensions in data array: %s! Aborting...', all_data[0].ndim)
            raise ValueError

        # Only 2- and 3-D data arrays can be written to GeoTIFF
        if data_to_write.ndim < 2 or data_to_write.ndim > 3:
            self.logger.error('Incorrect number of dimensions in data array: %s! Aborting...', all_data[0].ndim)
            raise ValueError

        # Fix number of dimensions. Should be 3!
        if data_to_write.ndim == 2:
            data_to_write = np.expand_dims(data_to_write, 0)  # If it's 2-D make it 3-D

        # Write image.
        dims = data_to_write.shape
        dataset = drv.Create(filename, dims[2], dims[1], dims[0], gdal.GDT_Float32)
        if dataset is None:
            self.logger.error('Error creating file: %s. Check the output path! Aborting...', filename)
            raise FileNotFoundError("Can't write file!")
        band = 0
        for data_slice in data_to_write:
            band += 1
            dataset.GetRasterBand(band).WriteArray(data_slice)

        # Prepare geokeys.
        gtype = 'EPSG:4326'
        cs = osr.GetWellKnownGeogCSAsWKT(gtype)
        dataset.SetProjection(cs)

        gt = [0, 1, 0, 0, 0, 1]  # Default value.

        # Pixel scale.
        # limits = {limit['@role']: int(limit['#text']) for limit in self._data_info['data']['projection']['limits']['limit']}
        gt[1] = longitudes[1] - longitudes[0]
        gt[5] = latitudes[1] - latitudes[0]

        # Top left pixel position.
        gt[0] = longitudes[0]
        gt[3] = latitudes[0]

        dataset.SetGeoTransform(gt)

        dataset = None

        self.logger.info('Done!')


class ImageShape:
    """ Provides reading/writing data from/to ESRI shapefiles.

    """
    def __init__(self, data_info):
        self.logger = logging.getLogger()
        self._data_info = data_info
        self.multiband_support = False

    def read(self, options):
        """Reads ESRI shapefile into an array.

        Arguments:
            options -- dictionary of read options
        """


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
        self.logger.info('  Writing ESRI Shapefile...')

        filename = make_filename(self._data_info, options)
        with shapefile.Writer(filename) as shape_writer:
            if values.ndim == 1:  # 1-D values means we have stations without a time dimension
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

        self.logger.info('  Done!')
