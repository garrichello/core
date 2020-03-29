"""Provides classes
    Data
"""
import logging
import numpy as np
from matplotlib.path import Path

from core.base.common import unlistify

GRID_TYPE_STATION = 'station'
GRID_TYPE_REGULAR = 'regular'
GRID_TYPE_IRREGULAR = 'irregular'

class Data:
    """ Provides common methods for data access modules (classes).
    """
    def __init__(self, data_info):
        self.logger = logging.getLogger()
        self._data_info = data_info
        self._read_result = {'@type': 'data'}   # Data arrays, grids and some additional information read in a child class.
        self._read_result['data'] = {}  # Contains data arrays read at each vertical level.
        self._data_by_segment = {}  # Data for each time segment for each vertical level.

    def _make_ROI(self):
        """ Creates region of interest for a given set of points.
        """

        ROI_lats_string = [p['@lat'] for p in self._data_info['data']['region']['point']]
        try:
            ROI_lats = [float(lat_string) for lat_string in ROI_lats_string]
        except ValueError:
            self.logger.error('Bad latitude value (not a number) in data: %s', self._data_info['data']['@uid'])
            raise
        ROI_lons_string = [p['@lon'] for p in self._data_info['data']['region']['point']]
        try:
            ROI_lons = [float(lon_string) for lon_string in ROI_lons_string]
        except ValueError:
            self.logger.error('Bad longitude value (not a number) in data: %s', self._data_info['data']['@uid'])
            raise

        self._ROI = [(lon, lat) for lon, lat in zip(ROI_lons, ROI_lats)]  # Region Of Interest.

        self._ROI_bounds = {'min_lon' : min(ROI_lons), 'max_lon' : max(ROI_lons),
                            'min_lat' : min(ROI_lats), 'max_lat' : max(ROI_lats)}

    def _make_ROI_mask(self, lons: np.ndarray, lats: np.ndarray):
        """ Creates a 2D-mask for a given region of interest.
        Arguments:
            lons -- longitudes of the masked area
            lats -- latitudes of the masked area
        Returns:
            mask -- 2D-mask for the area where True are masked values
        """
        if lons.ndim == 1:
            lon2d, lat2d = np.meshgrid(lons, lats)
            n_lats = lats.size
            n_lons = lons.size
        else:
            lon2d, lat2d = lons[:], lats[:]
            n_lats, n_lons = lons.shape
        lon_coords, lat_coords = lon2d.flatten(), lat2d.flatten()
        points = np.vstack((lon_coords, lat_coords)).T

        path = Path(self._ROI)
        mask = path.contains_points(points, radius=1e-5) # True is for the points inside the ROI
        mask = ~mask.reshape((n_lats, n_lons)) # True is masked so we need to inverse the mask

        return mask

    def _init_segment_data(self, level_name: str):
        """ Creates a dictionary to store data arrays for each time segment at a vertical level 'level_name'

        Argeuments:
            level_name -- vertical level name (string)
        """

        self._data_by_segment[level_name] = {}  # Data for each time segment at a specified vertical level.

    def _add_segment_data(self, level_name: str, values: np.ma.MaskedArray, time_grid: list, time_segment: dict):
        """ Stores data read for each time segment in a unified dictionary.

        Arguments:
            values -- masked data slice (MaskedArray)
            time_grid -- time grid (datetime list)
            time_segment -- (dictionary)
        """

        self._data_by_segment[level_name][time_segment['@name']] = {}
        self._data_by_segment[level_name][time_segment['@name']]['@values'] = values
        self._data_by_segment[level_name][time_segment['@name']]['@time_grid'] = time_grid
        self._data_by_segment[level_name][time_segment['@name']]['segment'] = time_segment

    def _add_metadata(self, longitude_grid: np.ndarray, latitude_grid: np.ndarray, fill_value: float, 
                      description: dict, grid_type=None, dimensions=None, meta=None):
        """ Stores main metadata for a read data array in a unified dictionary.

        Arguments:
            level_name -- level name (string)
            longitude_grid -- longitude grid (1- or 2-D ndarray)
            latitude_grid -- latitude grid (1- or 2-D ndarray)
            grid_type -- grid type: regular/irregular/station (string) - obsoleted, will be removed in future releases
            dimensions -- names of dimensions (list of strings)
            fill_value -- fill value in a masked array
            description -- data description
            meta -- additional metadata, currently used for passing names of weather stations (dictionary)
        """

        self._read_result['data'] = self._data_by_segment
        self._read_result['@longitude_grid'] = longitude_grid
        self._read_result['@latitude_grid'] = latitude_grid
        self._read_result['@grid_type'] = grid_type
        self._read_result['@dimensions'] = dimensions
        self._read_result['@fill_value'] = fill_value
        self._read_result['data']['description'] = description
        self._read_result['meta'] = {} if meta is None else meta

    def _get_result_data(self):
        """ Returns read data array accompanied with metadata.
        """

        return self._read_result

    def _transpose_dict(self, dict_of_lists: dict):
        """ Converts dictionary of lists to a list of dictionaries.

        Arguments:
            dict_of_lists -- dictionary of lists

        Returns:
            list_of_dicts -- list of dictionaries
        """

        if not isinstance(dict_of_lists, dict) or dict_of_lists is None:
            self.logger.debug('It\'s not a dictionary!')
            return dict_of_lists

        # Get lengths of lists.
        lengths = {}
        max_len = 0
        for key, value in dict_of_lists.items():
            if isinstance(value, list):
                val_len = len(value)
            else:
                val_len = 1  # If it's not a list set it's length to 1
            max_len = val_len if max_len < val_len else max_len
            lengths[key] = val_len

        # Check lengths of lists to be the same. Ignore elements of length 1.
        for val_len in lengths.values():
            if val_len != max_len and val_len > 1:
                self.logger.error('Lists in dictionary must be of the same length!')
                raise ValueError

        # Convert dictionary of lists to a list of dictionaries.
        # Copy elements of length 1 to all lists.
        list_of_dicts = []
        i = 0
        while i < max_len:
            single_dict = {}
            for key in dict_of_lists.keys():
                if lengths[key] > 1:
                    single_dict[key] = dict_of_lists[key][i]
                else:
                    single_dict[key] = unlistify(dict_of_lists[key])
            list_of_dicts.append(single_dict)
            i += 1

        return list_of_dicts
