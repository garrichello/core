"""Provides classes
    DataNetcdf
"""

from datetime import datetime
from netCDF4 import MFDataset, date2index, num2date, Dataset, date2num
from string import Template
import re
import numpy as np
import numpy.ma as ma
from matplotlib.path import Path
import os.path

from base.common import listify, unlistify, print

LONGITUDE_UNITS =  {'degrees_east', 'degree_east', 'degrees_E', 'degree_E', 'degreesE', 'degreeE', 'lon'}
LATITUDE_UNITS = {'degrees_north', 'degree_north', 'degrees_N', 'degree_N', 'degreesN', 'degreeN', 'lat'}
TIME_UNITS = {'since', 'time'}
NO_LEVEL_NAME = 'none'

class PercentTemplate(Template):
    """ Custom template for the string substitute method.
        It changes the template delimiter to %<template>%
    """
    delimiter = '%'
    pattern = r'''
    \%(?:
        (?P<escaped>%) |
        (?P<named>[_a-z][_a-z0-9]*)% |
        \b\B(?P<braced>) |
        (?P<invalid>)
    )
    '''

class DataNetcdf:
    """ Provides methods for reading and writing archives of netCDF files.
    """
    def __init__(self, data_info):
        self._data_info = data_info
        if self._data_info['data']['@type'] == 'dataset':
            self._set_ROI()

    def _set_ROI(self):
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
        
        self._ROI_limits = {'min_lon' : min(ROI_lons), 'max_lon' : max(ROI_lons),
                            'min_lat' : min(ROI_lats), 'max_lat' : max(ROI_lats)}

        self._ROI = [(lon, lat) for lon, lat in zip(ROI_lons, ROI_lats)]

    def read(self, options):
        """Reads netCDF-file into an array.

        Arguments:
            options -- dictionary of read options:
                ['segments'] -- time segments
                ['levels'] -- vertical levels

        Returns:
            result['array'] -- data array
        """

        # Segments must be a list or None.
        self._segments = listify(options['segments'])
        self._levels = listify(options['levels'])

        variable_indices = {} # Contains lists of indices for each dimension of the data variable in the domain to read.
        result = {} # Contains data arrays, grids and some additional information.
        result['data'] = {} # Contains data arrays being read from netCDF files at each vertical level.

        # Process each vertical level separately.
        for level_name in self._data_info["levels"]:
            print ('(DataNetcdf::read) Reading level: \'{0}\''.format(level_name))
            level_variable_name = self._data_info['levels'][level_name]['@level_variable_name']
            file_name_template = self._data_info['levels'][level_name]['@file_name_template'] # Template as in MDDB.
            percent_template = PercentTemplate(file_name_template) # Custom string template %keyword%.
            file_name_wildcard = percent_template.substitute({'year' : '????'}) # Create wildcard-ed template

            netcdf_root = MFDataset(file_name_wildcard, check=True)

            data_variable = netcdf_root.variables[self._data_info['data']['variable']['@name']] # Data variable.

            # Determine indices of longitudes. 
            longitude_variable = unlistify(netcdf_root.get_variables_by_attributes(units=lambda v: v in LONGITUDE_UNITS))
            if longitude_variable.ndim == 1:
                lon_grid_type = 'regular'
                lons = longitude_variable[:]
                if lons.max() > 180:
                    lons = ((lons + 180.0) % 360.0) - 180.0 # Switch from 0-360 to -180-180 grid
                longitude_indices = np.nonzero([ge and le for ge, le in 
                        zip(lons >= self._ROI_limits['min_lon'], lons <= self._ROI_limits['max_lon'])])[0]
            variable_indices[longitude_variable.name] = longitude_indices
            longitude_grid = lons[longitude_indices]

            # Determine indices of latitudes.
            latitude_variable = unlistify(netcdf_root.get_variables_by_attributes(units=lambda v: v in LATITUDE_UNITS))
            if longitude_variable.ndim == 1:
                lat_grid_type = 'regular'
                lats = latitude_variable[:]
                latitude_indices = np.nonzero([ge and le for ge, le in 
                        zip(lats >= self._ROI_limits['min_lat'], lats <= self._ROI_limits['max_lat'])])[0]
            variable_indices[latitude_variable.name] = latitude_indices
            latitude_grid = lats[latitude_indices]
            
            if lon_grid_type == lat_grid_type:
                grid_type = lon_grid_type
            else:
                print ('(DataNetcdf::read) Error! Longitude and latitude grids are not match! Aborting.')
                raise ValueError

            # Create ROI mask.
            lon2d, lat2d = np.meshgrid(lons, lats)
            lon_coords, lat_coords = lon2d.flatten(), lat2d.flatten()
            points = np.vstack((lon_coords, lat_coords)).T

            path = Path(self._ROI)
            ROI_mask = path.contains_points(points) # True is for the points inside the ROI
            ROI_mask = ~ROI_mask.reshape((lats.size, lons.size)) # True is masked so we need to inverse the mask

            # Determine index of the current vertical level to read data variable.
            if level_variable_name != NO_LEVEL_NAME:
                try:
                    level_variable = netcdf_root.variables[level_variable_name]
                except KeyError:
                    print ('(DataNetcdf::read) Level variable \'{0}\' is not found in files. Aborting!'.format(
                            level_variable_name))
                    raise
                try:
                    level_index = level_variable[:].astype('str').tolist().index(level_name)
                except KeyError:
                    print ('(DataNetcdf::read) Level \'{0}\' is not found in level variable \'{1}\'. Aborting!'.format(
                            level_name, level_variable_name))
                    raise                    
                variable_indices[level_variable.name] = level_index
            else:
                level_index = None

            time_variable = unlistify(netcdf_root.get_variables_by_attributes(
                    units=lambda v: True in [tu in v for tu in TIME_UNITS]))
            
            # Process each time segment separately.
            data_by_segment = {} # Contains data array for each time segment.
            for segment in self._segments:
                print ('(DataNetcdf::read) Reading time segment \'{0}\''.format(segment['@name']))

                segment_start = datetime.strptime(segment['@beginning'], '%Y%m%d%H')
                segment_end = datetime.strptime(segment['@ending'], '%Y%m%d%H')
                time_idx_range = date2index([segment_start, segment_end], time_variable)
                variable_indices[time_variable._name] = np.arange(time_idx_range[0], time_idx_range[1])
                time_values = time_variable[variable_indices[time_variable._name]] # Raw time values.
                time_grid = num2date(time_values, time_variable.units) # Time grid as a datetime object.
                
                dd = data_variable.dimensions # Names of dimensions of the data variable.

                # Here we actually read the data array from the file for all lons and lats (it's faster to read everything).
                # TODO: Support for different number of dimensions of data variable is needed.
                # TODO: Are we sure that the last two dimensions are lat and lon correspondingly?
                data_slice = data_variable[variable_indices[dd[0]], variable_indices[dd[1]], :, :]
                
                # Mask all points outside the ROI mask for all times.
                # TODO: Tuple (time_grid.size, 1, 1) must correspond to number of dimensions of data variable
                ROI_mask_time = np.tile(ROI_mask, (time_grid.size, 1, 1)) # Propagate ROI mask along the time dimension.
                masked_data_slice = ma.MaskedArray(data_slice, mask=ROI_mask_time, fill_value=data_variable._FillValue) # Create masked array using ROI mask.

                # Remove level variable name from the list of data dimensions if it is present
                if level_variable_name != NO_LEVEL_NAME:
                    data_dim_names = [name for name in dd if name != level_variable_name]
                
                data_by_segment[segment['@name']] = {}
                data_by_segment[segment['@name']]['@values'] = masked_data_slice
                data_by_segment[segment['@name']]['@units'] = data_variable.units
                data_by_segment[segment['@name']]['@dimensions'] = data_dim_names
                data_by_segment[segment['@name']]['@time_grid'] = time_grid
                data_by_segment[segment['@name']]['segment'] = segment
            
            result['data'][level_name] = data_by_segment
            result['@longitude_grid'] = lons #longitude_grid
            result['@latitude_grid'] = lats #latitude_grid
            result['@grid_type'] = grid_type

        return result

    def write(self, values, options):
        """Writes data array into a netCDF file.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options:
                ['level'] -- vertical level name 
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
        """    
        
        print('(DataNetcdf::write) Writing netCDF...')

        # Contrcut file name
        (file_root, file_ext) = os.path.splitext(self._data_info['data']['file']['@name'])
        filename = '{}_{}_{}-{}{}'.format(file_root, options['level'], 
            options['segment']['@beginning'], options['segment']['@ending'], file_ext)

        # Create netCDF file.
        root = Dataset(filename, 'w', format='NETCDF3_64BIT_OFFSET')

        # Define dimensions.
        lon = root.createDimension('lon', options['longitudes'].size)
        lat = root.createDimension('lat', options['latitudes'].size)
        if options['times'] is not None:
            time = root.createDimension('time', options['times'].size)
            times = root.createVariable('time', 'f8', ('time'))
            times.units = 'hours since 1979-1-1 00:00:0.0'
            times.long_name = 'Time'
        
        # Define variables.
        latitudes = root.createVariable('lat', 'f4', ('lat'))
        longitudes = root.createVariable('lon', 'f4', ('lon'))
        if options['times'] is not None:
            data = root.createVariable('data', 'f4', ('time', 'lat', 'lon'), fill_value=values.fill_value)
        else:
            data = root.createVariable('data', 'f4', ('lat', 'lon'), fill_value=values.fill_value)

        # Set global attributes.
        root.Title = options['description']['@title']
        root.Conventions = 'CF'
        root.Source = 'Generated by CLIMATE Web-GIS'

        # Set variables attributes.
        latitudes.units = 'degrees_north'
        latitudes.long_name = 'Latitude'
        longitudes.units = 'degrees_east'
        longitudes.long_name ='Longitude'
        data.units = options['description']['@units']
        data.long_name = '{} {}'.format(options['description']['@title'], options['description']['@name'])

        # Write variables.
        if options['times'] is not None:
            pass
        longitudes[:] = options['longitudes']
        latitudes[:] = options['latitudes']
        data[:] = ma.filled(values, fill_value=values.fill_value)

        pass
