"""Provides classes: MFDataset, Variable
"""

import glob
from pyhdf.SD import SD, SDC
import re
import numpy as np

from base.common import print


class MFDataset:
    """ Provides access to multifile HDF files
    """
    def __init__(self, file_name_wildcard):
        self._file_name_wildcard = file_name_wildcard
        
        self._files = {}
        
        self.variables = {}

        datasets = None
        meta = None
        for file_name in glob.iglob(file_name_wildcard):
            self._files[file_name] = SD(file_name, SDC.READ)
            new_datasets = self._files[file_name].datasets()
            if datasets is None:
                datasets = new_datasets
            else:
                if datasets != new_datasets:
                    print('(MFDataset::__init__) Warning! Datasets list in the file {} is different!'.format(file_name))
                    if set(new_datasets.keys()) > set(datasets.keys()):
                        diff = set(new_datasets.keys()) - set(datasets.keys())
                        print('Extra dataset: {}'.format(diff))
                    else:
                        diff = set(datasets.keys()) - set(new_datasets.keys())
                        print('Missing dataset: {}'.format(diff))
            if meta is None:
                meta = self._get_metadata(self._files[file_name])
                self._longitudes = meta['longitudes']
                self._latitudes = meta['latitudes']
            pass

        for dataset_name in datasets.keys():
            self.variables[dataset_name] = Variable(dataset_name, self._files)


        pass

    def _list_to_dict(self, pairlist):
        pairdict = {}
        group_list = []
        in_group = False
        
        for pair in pairlist:
            if pair[0] == 'GROUP' and not in_group:
                group_name = pair[1]
                group_list = []
                in_group = True
            elif pair[0] == 'END_GROUP' and pair[1] == group_name:
                pairdict[group_name] = self._list_to_dict(group_list)
                in_group = False
            else:
                group_list.append(pair)
        
        if len(pairdict) == 0:
            pairdict = dict(group_list)

        return pairdict

    def _get_metadata(self, file):

        meta = {}

        # Read global attributes.
        fattrs = file.attributes()
        stringmeta = fattrs["StructMetadata.0"]
        
        listmeta = stringmeta.replace('\x00', '').split('\n')
        listmeta.remove('END')
        listmeta.remove('')
        pairmeta = [s.strip().split('=') for s in listmeta]
        dictmeta = self._list_to_dict(pairmeta)

        # The needed information is in a global attribute 'StructMetadata.0'.  
        # Use regular expressions to extract it.
        # Upper left corner.
        ul_regex = re.compile(r'''UpperLeftPointMtrs=\(
                                  (?P<upper_left_x>[+-]?\d+\.\d+)
                                  ,
                                  (?P<upper_left_y>[+-]?\d+\.\d+)
                                  \)''', re.VERBOSE)
        match = ul_regex.search(gridmeta)
        lon0 = np.float(match.group('upper_left_x')) / 1.0E6
        lat0 = np.float(match.group('upper_left_y')) / 1.0E6
        # Bottom right corner.
        lr_regex = re.compile(r'''LowerRightMtrs=\(
                                  (?P<lower_right_x>[+-]?\d+\.\d+)
                                  ,
                                  (?P<lower_right_y>[+-]?\d+\.\d+)
                                  \)''', re.VERBOSE)
        match = lr_regex.search(gridmeta)
        lon1 = np.float(match.group('lower_right_x')) / 1.0E6
        lat1 = np.float(match.group('lower_right_y')) / 1.0E6
        # Longitude dimension size.
        xdim_regex = re.compile(r'''XDim=(?P<x_dim>\d+)''', re.VERBOSE)
        match = xdim_regex.search(gridmeta)
        n_lon = np.float(match.group('x_dim'))
        # Latitude dimension size.
        ydim_regex = re.compile(r'''YDim=(?P<y_dim>\d+)''', re.VERBOSE)
        match = ydim_regex.search(gridmeta)
        n_lat = np.float(match.group('y_dim'))
        # Steps.
        lon_inc = (lon1 - lon0) / n_lon
        lat_inc = (lon1 - lon0) / n_lon
        # Generate longitude and latitude grids.
        meta['longitudes'] = np.linspace(lon0, lon0 + lon_inc*n_lon, n_lon)
        meta['latitudes'] = np.linspace(lat0, lat0 + lat_inc*n_lat, n_lat)

        return meta

    def get_longitude_variable(self):
        return self._longitudes

    def get_latitude_variable(self):
        return self._latitudes

    def get_time_variable(self):
        pass


class Variable:
    """ Provides access to HDF variables
    """
    def __init__(self, dataset_name, files):
        self._dataset_name = dataset_name
        self._files = files
        pass