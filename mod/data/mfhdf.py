"""Provides classes: MFDataset, Variable
"""

import glob
from pyhdf.SD import SD, SDC
from datetime import datetime
import numpy as np
from collections.abc import Sequence

from base.common import print, list_remove_all, listify


def date2index(datetime_values, time_variable, select=None):
    ''' Converts datetime values into indices in time_variable.
    Nearest mode only!

    Arguments:
        datetime_values -- datetime value or list of values.
        time_variable -- time grid where datetime_values positions are searched for.

    Result:
        indices -- indices of the elements of the time_variable nearest to the datetime_values.
    '''

    indices = []
    for value in listify(datetime_values):
        idx = np.searchsorted(time_variable, value, side='right')
        indices.append(idx if (time_variable[idx] - value) < (value - time_variable[idx - 1]) else idx - 1)

    return indices


class MFDataset:
    """ Provides access to multifile HDF files.
    """
    def __init__(self, file_name_wildcard, check=False):
        self._file_name_wildcard = file_name_wildcard

        self._files = []
        self.variables = {}

        # Open all files and read metadata to create longitude, latitude and time grids.
        datasets = None
        datetime_range = []
        first_file = True
        print('(MFHDF::__init__) Reading files info...')
        for file_name in glob.iglob(file_name_wildcard):
            hdf_file = SD(file_name, SDC.READ)  # Open file.
            self._files.append(hdf_file)  # Store HDF file handler.
            if check:
                new_datasets = hdf_file.datasets()  # Get datasets info.
                # All files should have the same set of datasets.
                if datasets is None:
                    datasets = new_datasets
                else:
                    if datasets != new_datasets:  # If not - print a warning.
                        print('(MFDataset::__init__) Warning! Datasets list in the file {} is different!'.format(file_name))
                        if set(new_datasets.keys()) > set(datasets.keys()):
                            diff = set(new_datasets.keys()) - set(datasets.keys())
                            print('Extra dataset: {}'.format(diff))
                        else:
                            diff = set(datasets.keys()) - set(new_datasets.keys())
                            print('Missing dataset: {}'.format(diff))
            elif datasets is None:
                datasets = hdf_file.datasets()  # Get datasets info.

            # Read datetime range for each file (because it differs)
            datetime_range.append(self._get_datetime_range(hdf_file))

            # We read spatial grids only from the first file (should be the same for all files).
            if first_file:
                spatial_grids = self._get_spatial_grids(hdf_file)
                self._longitudes = spatial_grids['longitudes']
                self._latitudes = spatial_grids['latitudes']
                first_file = False

        print('(MFHDF::__init__) Done!')
        # Let's take the beginning of the range for each file to construct a time grid.
        self._times = [beginning for beginning, ending in datetime_range]

        # Sort files by date and time.
        sorter = np.argsort(self._times)
        self._files = [self._files[i] for i in sorter]
        self._times = np.array([self._times[i] for i in sorter])

        for dataset_name in datasets.keys():
            self.variables[dataset_name] = Variable(dataset_name, self._files)

    def _meta_to_list(self, meta):
        ''' Convert metadata in string format into a list of key-value pairs.

        Uses:
            common.list_remove_all()

        Arguments:
            structmetadata -- a string with hierarchically organized HDF metadata

        Result: a list of key-value pairs
        '''

        list_ = meta.replace('\x00', '').split('\n')
        list_ = list_remove_all(list_, 'END')
        list_ = list_remove_all(list_, '')

        return [s.split('=', 1) for s in list_]

    def _list_to_dict(self, pairlist):
        ''' Recursively converts a list of hierarchically organized key-value pairs into a dictionary.

        Arguments:
            pairlist -- list with key-value pairs.

        Result:
            pairdict -- dictionary with key-value pairs.
        '''

        pairdict = {}
        in_group = False

        for key, value in pairlist:
            key = key.strip()
            value = value.strip()
            if (key == 'GROUP' or key == 'OBJECT') and not in_group:
                group_name = value
                group_list = []
                in_group = True
            elif (key == 'END_GROUP' or key == 'END_OBJECT') and value == group_name:
                pairdict[group_name] = self._list_to_dict(group_list)
                in_group = False
            elif in_group:
                group_list.append([key, value])
            else:
                pairdict[key] = value

        return pairdict

    def _get_spatial_grids(self, file):
        ''' Reads metadata from HDF file and returns longitude and latitude grids.
        Arguments:
            file -- HDF file handle.

        Result:
            spatial_grids -- dictionary {'longitude': longitude_grid, 'latitude': latitude_grid}.
        '''

        spatial_grids = {}

        # Get indices of global attributes.
        index_struct_meta = file.attr('StructMetadata.0').index()
        index_core_meta = file.attr('CoreMetadata.0').index()

        # The needed information is in a global attribute 'StructMetadata.0'.
        structmeta = file.attr(index_struct_meta).get()
        structlist = self._meta_to_list(structmeta)
        structdict = self._list_to_dict(structlist)

        # The additional needed information is in a global attribute 'CoreMetadata.0'.
        coremeta = file.attr(index_core_meta).get()
        corelist = self._meta_to_list(coremeta)
        coredict = self._list_to_dict(corelist)

        # Corners of the area
        spatial_container = coredict['INVENTORYMETADATA']['SPATIALDOMAINCONTAINER']
        bounding_rectangle = spatial_container['HORIZONTALSPATIALDOMAINCONTAINER']['BOUNDINGRECTANGLE']
        lon0 = np.float(bounding_rectangle['WESTBOUNDINGCOORDINATE']['VALUE'])
        lat0 = np.float(bounding_rectangle['NORTHBOUNDINGCOORDINATE']['VALUE'])
        lon1 = np.float(bounding_rectangle['EASTBOUNDINGCOORDINATE']['VALUE'])
        lat1 = np.float(bounding_rectangle['SOUTHBOUNDINGCOORDINATE']['VALUE'])

        # Dimensions.
        n_lon = np.int(structdict['GridStructure']['GRID_1']['XDim'])
        n_lat = np.int(structdict['GridStructure']['GRID_1']['YDim'])

        # Steps.
        lon_inc = (lon1 - lon0) / n_lon
        lat_inc = (lat1 - lat0) / n_lat

        # Generate longitude and latitude grids.
        spatial_grids['longitudes'] = np.linspace(lon0, lon0 + lon_inc * n_lon, n_lon)
        spatial_grids['latitudes'] = np.linspace(lat0, lat0 + lat_inc * n_lat, n_lat)

        return spatial_grids

    def _get_datetime_range(self, file):
        ''' Reads metadata from HDF file and returns a time range.

        Arguments:
            file -- HDF file handle.

        Result:
            datetime_range -- tuple, datetime range (begin, end).
        '''

        # Get indexx of a global attribute CoreMetadata.0 containing datetime info.
        index_core_meta = file.attr('CoreMetadata.0').index()
        coremeta = file.attr(index_core_meta).get()
        corelist = self._meta_to_list(coremeta)
        coredict = self._list_to_dict(corelist)

        # Get date and time range values
        rangedatetime_container = coredict['INVENTORYMETADATA']['RANGEDATETIME']
        date0_string = rangedatetime_container['RANGEBEGINNINGDATE']['VALUE']
        time0_string = rangedatetime_container['RANGEBEGINNINGTIME']['VALUE']
        if len(time0_string) > 10:
            datetime0 = datetime.strptime(date0_string + time0_string, '"%Y-%m-%d""%H:%M:%S.%f"')
        else:
            datetime0 = datetime.strptime(date0_string + time0_string, '"%Y-%m-%d""%H:%M:%S"')
        date1_string = rangedatetime_container['RANGEENDINGDATE']['VALUE']
        time1_string = rangedatetime_container['RANGEENDINGTIME']['VALUE']
        if len(time1_string) > 10:
            datetime1 = datetime.strptime(date1_string + time1_string, '"%Y-%m-%d""%H:%M:%S.%f"')
        else:
            datetime1 = datetime.strptime(date1_string + time1_string, '"%Y-%m-%d""%H:%M:%S"')
        datetime_range = (datetime0, datetime1)

        return datetime_range

    def get_longitude_variable(self):
        return self._longitudes

    def get_latitude_variable(self):
        return self._latitudes

    def get_time_variable(self):
        return self._times


class Variable(Sequence):
    """ Provides access to HDF variables
    """
    def __init__(self, dataset_name, files):
        self._dataset_name = dataset_name
        self._files = files

        dataset = files[0].select(dataset_name)
        attributes = dataset.attributes()

        self.dimensions = ['Time']
        self._shape = [len(files)]
        for i in range(len(dataset.dimensions())):
            name, length, _, _ = dataset.dim(i).info()
            self.dimensions.append(name.split(':')[0])
            self._shape.append(length)
        self.ndim = len(self.dimensions)
        self._FillValue = attributes['_FillValue']
        layers = {int(key.split(' ')[1]): value for key, value in attributes.items() if key.find('Layer') != -1}
        if len(layers) == 0 and len(attributes) > 4:  # No layers and many attributes means we have legend as attributes
            inv_attributes = {value: key for key, value in attributes.items() if type(value) != list}
            layers = {i: inv_attributes[i] for i in range(attributes['valid_range'][0], attributes['valid_range'][1] + 1)}
        self._levels = np.array([None] * len(layers))
        for i in range(len(layers)):
            self._levels[i] = layers[i]
        # Set fake level variable name
        if self.ndim == 4:  # Dataset contains layers
            self._level_variable_name = self.dimensions[3]
        else:
            self._level_variable_name = None

    def __getitem__(self, key):
        if isinstance(key, slice):
            pass
        elif isinstance(key, int):
            pass
        elif isinstance(key, tuple):
            file_indices = key[0]  # We know it because we did it (see init)
            s = [int(key[i][0]) for i in range(1, len(key))]
            c = [len(key[i]) for i in range(1, len(key))]
            data = []
            for i in file_indices:
                dataset = self._files[i].select(self._dataset_name)
                data.append(dataset.get(start=s, count=c))
        else:
            raise TypeError("Invalid argument type.")

        return np.array(data)

    def __len__(self):
        return np.prod(self._shape)

    def get_level_variable(self):
        return self._levels

    def get_level_variable_name(self):
        return self._level_variable_name
