"""Provides classes
    Data
"""

import numpy as np
from matplotlib.path import Path


class Data:
    """ Provides common methods for data access modules (classes).
    """
    def __init__(self, data_info):
        self._data_info = data_info

    def _create_ROI_mask(self, lons, lats):
        """ Creates a 2D-mask for a given region of interest.
        Arguments:
            lons -- longitudes of the masked area
            lats -- latitudes of the masked area
        Returns:
            mask -- 2D-mask for the area where True are masked values
        """

        ROI_lats_string = [p['@lat'] for p in self._data_info['data']['region']['point']]
        try:
            ROI_lats = [float(lat_string) for lat_string in ROI_lats_string]
        except ValueError:
            print('(DataNetcdf::__init__): Bad latitude value (not a number) in data: {0}'.format(self._data_info['data']['@uid']))
            raise
        ROI_lons_string = [p['@lon'] for p in self._data_info['data']['region']['point']]
        try:
            ROI_lons = [float(lon_string) for lon_string in ROI_lons_string]
        except ValueError:
            print('(DataNetcdf::__init__): Bad longitude value (not a number) in data: {0}'.format(self._data_info['data']['@uid']))
            raise

        ROI = [(lon, lat) for lon, lat in zip(ROI_lons, ROI_lats)]  # Region Of Interest.

#        self._ROI_limits = {'min_lon' : min(ROI_lons), 'max_lon' : max(ROI_lons),
#                            'min_lat' : min(ROI_lats), 'max_lat' : max(ROI_lats)}

#        latitude_indices = np.nonzero([ge and le for ge, le in 
#                zip(lats >= self._ROI_limits['min_lat'], lats <= self._ROI_limits['max_lat'])])[0]
#        longitude_indices = np.nonzero([ge and le for ge, le in 
#                zip(lons >= self._ROI_limits['min_lon'], lons <= self._ROI_limits['max_lon'])])[0]

        lon2d, lat2d = np.meshgrid(lons, lats)
        lon_coords, lat_coords = lon2d.flatten(), lat2d.flatten()
        points = np.vstack((lon_coords, lat_coords)).T

        path = Path(ROI)
        mask = path.contains_points(points) # True is for the points inside the ROI
        mask = ~mask.reshape((lats.size, lons.size)) # True is masked so we need to inverse the mask

        return mask