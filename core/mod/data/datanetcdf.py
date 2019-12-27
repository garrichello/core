"""Provides classes
    DataNetcdf
"""
from string import Template
from datetime import datetime
import re

from copy import copy
from netCDF4 import MFDataset, date2index, num2date, Dataset, MFTime
import numpy as np
import numpy.ma as ma

from core.base.common import listify, unlistify
from .data import Data, GRID_TYPE_REGULAR, GRID_TYPE_IRREGULAR

LONGITUDE_UNITS = {'degrees_east', 'degree_east', 'degrees_E', 'degree_E',
                   'degreesE', 'degreeE', 'lon'}
LATITUDE_UNITS = {'degrees_north', 'degree_north', 'degrees_N', 'degree_N',
                  'degreesN', 'degreeN', 'lat'}
TIME_UNITS = {'since', 'time'}
NO_LEVEL_NAME = 'none'
WILDCARDS = {'year': '????', 'mm': '??', 'year1': '????', 'year2': '????', 'year1s-4': '????', 'year2s-4': '????', 'year1s1': '????', 'year2s1': '????'}

class PercentTemplate(Template):
    """ Custom template for the string substitute method.
        It changes the template delimiter to %<template>%
    """
    delimiter = '%'
    pattern = r'''
    \%(?:
        (?P<escaped>%) |
        (?P<named>[_a-z][_a-z0-9\-]*)% |
        \b\B(?P<braced>) |
        (?P<invalid>)
    )
    '''


class DataNetcdf(Data):
    """ Provides methods for reading and writing archives of netCDF files.
    """
    def __init__(self, data_info):
        super().__init__(data_info)
        self._data_info = data_info

        self.file_name_wildcard = ''
        self.netcdf_root = None

    def __del__(self):
        if self.netcdf_root is not None:
            self.netcdf_root.close()

    def _get_longitudes(self, nc_root):
        longitude_variable = unlistify(nc_root.get_variables_by_attributes(units=lambda v: v in LONGITUDE_UNITS))
        lons = longitude_variable[:]
        if longitude_variable.ndim == 1:
            grid_type = GRID_TYPE_REGULAR
            if lons.max() > 180:
                lons = ((lons + 180.0) % 360.0) - 180.0  # Switch from 0-360 to -180-180 grid
        else:
            grid_type = GRID_TYPE_IRREGULAR

        return (lons, longitude_variable.name, grid_type)

    def _get_latitudes(self, nc_root):
        latitude_variable = unlistify(nc_root.get_variables_by_attributes(units=lambda v: v in LATITUDE_UNITS))
        lats = latitude_variable[:]
        if latitude_variable.ndim == 1:
            grid_type = GRID_TYPE_REGULAR
        else:
            grid_type = GRID_TYPE_IRREGULAR

        return (lats, latitude_variable.name, grid_type)

    def _get_levels(self, nc_root, level_name, level_variable_name):
        if level_variable_name != NO_LEVEL_NAME:
            try:
                level_variable = nc_root.variables[level_variable_name]  # pylint: disable=E1136
            except KeyError:
                self.logger.error('Level variable \'%s\' is not found in files. Aborting!',
                                  level_variable_name)
                raise
            if len(level_variable) == 1 and level_name == level_variable.units:
                # For reading data files where only one level is present, such as 'none', 'sfc', 'msl'...
                level_index = 0
            else:
                try:
                # Little hack: convert level name from metadata database to float and back to string type
                # to be able to search it correctly in float type level variable converted to string.
                # Level variable is also primarily converted to float to 'synchronize' types.
                    level_index = level_variable[:].astype('float').astype('str').tolist().index(str(float(re.findall(r'\d+', level_name)[0])))
                except KeyError:
                    self.logger.error('Level \'%s\' is not found in level variable \'%s\'. Aborting!',
                                      level_name, level_variable_name)
                    raise
        else:
            level_index = None

        return level_index

    def read(self, options):
        """Reads netCDF-file into an array.

        Arguments:
            options -- dictionary of read options:
                ['segments'] -- time segments
                ['levels'] -- vertical levels

        Returns:
            result['array'] -- data array
        """

        self.logger.info('Reading NetCDF data...')
        if self._data_info['@data_type'] == 'dataset':
            self.logger.info('[Dataset: %s, resolution: %s, scenario: %s, time_step: %s]',
                             self._data_info['data']['dataset']['@name'], self._data_info['data']['dataset']['@resolution'],
                             self._data_info['data']['dataset']['@scenario'], self._data_info['data']['dataset']['@time_step']
                            )
        if self._data_info['@data_type'] == 'raw':
            self.logger.info('[File: %s, type: %s]',
                             self._data_info['data']['file']['@name'], self._data_info['data']['file']['@type'])
        self.logger.info('[Variable: %s]', self._data_info['data']['variable']['@name'])

        # Levels must be a list or None.
        levels_to_read = listify(options['levels'])
        if levels_to_read is None:
            levels_to_read = listify(self._data_info['data']['levels']['@values'])  # Read all levels if nothing specified.
        # Segments must be a list or None.
        segments_to_read = listify(options['segments'])
        if segments_to_read is None:
            segments_to_read = listify(self._data_info['data']['time']['segment'])  # Read all levels if nothing specified.

        variable_indices = {}  # Contains lists of indices for each dimension of the data variable in the domain to read.

        self._make_ROI()

        # Process each vertical level separately.
        for level_name in levels_to_read:
            dd = [] # Data variable's dimensions list.

            self.logger.info('Vertical level: \'%s\'', level_name)
            level_variable_name = self._data_info['data']['levels'][level_name]['@level_variable_name']

            # Determine the index of the current vertical level in the level variable.
            level_index = self._get_levels(netcdf_root, level_name, level_variable_name)
            if level_index is not None:
                variable_indices[level_variable_name] = [level_index]
                dd.append(level_variable_name)

            data_scale = self._data_info['data']['levels'][level_name]['@scale']
            data_offset = self._data_info['data']['levels'][level_name]['@offset']

            file_name_template = self._data_info['data']['levels'][level_name]['@file_name_template']  # Template as in MDDB.
            percent_template = PercentTemplate(file_name_template)  # Custom string template %keyword%.
            file_name_wildcard = percent_template.substitute(WILDCARDS)  # Create wildcard-ed template

            # Kind of a caching for netcdf_root to save time working at the same vertical level.
            self.logger.info('Open files...')
            if self.file_name_wildcard != file_name_wildcard:  # If this is the first time we see this wildcard...
                try:
                    netcdf_root = MFDataset(file_name_wildcard, check=True)
                except OSError:
                    try:
                        netcdf_root = MFDataset(file_name_wildcard, check=True, aggdim='time')
                    except OSError:
                        netcdf_root = MFDataset(file_name_wildcard, check=True, aggdim='initial_time0_hours')
                self.file_name_wildcard = file_name_wildcard  # we store wildcard...
                self.netcdf_root = netcdf_root                # and netcdf_root.
            else:
                netcdf_root = self.netcdf_root  # Otherwise we take its "stored value".
            self.logger.info('Done!')

            data_variable = netcdf_root.variables[self._data_info['data']['variable']['@name']]  # Data variable. pylint: disable=E1136
            data_variable.set_auto_mask(False)

            self.logger.info('Get grids...')
            # Determine indices of longitudes.
            lons, longitude_variable_name, lon_grid_type = self._get_longitudes(netcdf_root)
            if lon_grid_type == GRID_TYPE_REGULAR:  # For regular grid we will read only rectangular area bounding ROI.
                longitude_indices = np.nonzero([ge and le for ge, le in
                                                zip(lons >= self._ROI_bounds['min_lon'], 
                                                    lons <= self._ROI_bounds['max_lon'])])[0]
                longitude_grid = lons[longitude_indices]
            else:
                longitude_indices = np.arange(lons.shape[-1])  # For irregular grids we will read the WHOLE area.
                longitude_grid = lons[:]
            variable_indices[longitude_variable_name] = longitude_indices
            dd.append(longitude_variable_name)

            # Determine indices of latitudes.
            lats, latitude_variable_name, lat_grid_type = self._get_latitudes(netcdf_root)
            if lon_grid_type == GRID_TYPE_REGULAR:  # For regular grid we will read only rectangular area bounding ROI.
                latitude_indices = np.nonzero([ge and le for ge, le in
                                               zip(lats >= self._ROI_bounds['min_lat'], 
                                                   lats <= self._ROI_bounds['max_lat'])])[0]
                latitude_grid = lats[latitude_indices]
            else:
                latitude_indices = np.arange(lats.shape[-2])  # For irregular grids we will read the WHOLE area.
                latitude_grid = lats[:]
            variable_indices[latitude_variable_name] = latitude_indices
            dd.insert(-1, latitude_variable_name)

            # Check if grids types are the same.
            if lon_grid_type == lat_grid_type:
                grid_type = lon_grid_type
            else:
                self.logger.error('Error! Longitude and latitude grids are not match! Aborting.')
                raise ValueError

            # A small temporary hack.
            # TODO: Dataset DS131, T62 grid variables has a dimension 'forecast_time1'. Now I set it
            # to the first element (whatever it is), but in the future it somehow should be selected by a user
            # in the GUI (may be) and passed here through an XML task-file.
            variable_indices['forecast_time1'] = [0]

            time_variable = unlistify(netcdf_root.get_variables_by_attributes(
                units=lambda v: True in [tu in v for tu in TIME_UNITS] if v is not None else False))
            try:
                calendar = time_variable.calendar
            except AttributeError:
                calendar = 'standard'
            if len(netcdf_root._files) > 1:  # Skip if there only one file  # pylint: disable=W0212, E1101
                time_variable = MFTime(time_variable, calendar=calendar)  # Apply multi-file support to the time variable
            if time_variable is not None:
                dd.insert(-2, time_variable._name) # pylint: disable=W0212, E1101

            self.logger.info('Done!')

            # Create ROI mask.
            ROI_mask = self._make_ROI_mask(longitude_grid, latitude_grid)

            # Process each time segment separately.
            self._init_segment_data(level_name)  # Initialize a data dictionary for the vertical level 'level_name'.
            for segment in segments_to_read:
                self.logger.info('Time segment \'%s\' (%s-%s)',
                                 segment['@name'], segment['@beginning'], segment['@ending'])

                segment_start = datetime.strptime(segment['@beginning'], '%Y%m%d%H')
                segment_end = datetime.strptime(segment['@ending'], '%Y%m%d%H')
                time_idx_start = date2index(segment_start, time_variable, select='after')
                time_idx_end = date2index(segment_end, time_variable, select='before')
                time_idx_range = [time_idx_start, time_idx_end]
                if time_idx_range[1] == 0:
                    self.logger.error('''Error! The end of the time segment is before the first time in the dataset.
                                         Aborting!''')
                    raise ValueError
                variable_indices[time_variable._name] = np.arange(time_idx_range[0], time_idx_range[1]+1)  # pylint: disable=W0212, E1101
                time_values = time_variable[variable_indices[time_variable._name]]  # Raw time values.  # pylint: disable=W0212, E1101
                time_grid = num2date(time_values, time_variable.units)  # Time grid as a datetime object.  # pylint: disable=E1101

                # Searching for a gap in longitude indices. Normally all steps should be equal to 1.
                # If there is a step longer than 1, we suppose it's a gap due to a shift from 0-360 to -180-180 grid.
                # So instead of a sigle patch in a 0-360 longitude space we should deal with two patches
                # in a -180-180 longitude space: one to the left of the 0 meridian, and the other to the right of it.
                # So we search for the gap's position and prepare to read two parts of data.
                # Then we stack them reversely (left to the right) and fix the longitude grid.
                # Thus we will have a single data patch with the uniform logitude grid.
                lon_gap_mode = False  # Normal mode.
                if grid_type == GRID_TYPE_REGULAR:
                    for i in range(len(variable_indices[longitude_variable_name])-1):
                        if variable_indices[longitude_variable_name][i+1]-variable_indices[longitude_variable_name][i] > 1:
                            lon_gap_mode = True  # Gap mode! Set the flag! :)
                            lon_gap_position = i  # Index of the gap.

                # Get start (first) and stop (last) indices for each dimension.
                start_index = [variable_indices[dd[i]][0] for i in range(len(dd))]
                stop_index = [variable_indices[dd[i]][-1]+1 for i in range(len(dd))]
                if lon_gap_mode:  # For gap mode we need start and stop indices for the second data part.
                    self.logger.info('Longitude gap detected. Gap mode activated!')
                    lon_index_pos = dd.index(longitude_variable_name)  # Position of the longitude indices in dimensions list.
                    start_index_2 = copy(start_index)
                    stop_index_2 = copy(stop_index)
                    stop_index[lon_index_pos] = \
                        variable_indices[longitude_variable_name][lon_gap_position]+1  # First part ends here.
                    start_index_2[lon_index_pos] = \
                        variable_indices[longitude_variable_name][lon_gap_position+1]  # Second part starts here.

                # Here we actually read the data array from the file for all lons and lats (it's faster to read everything).
                # And mask all points outside the ROI mask for all times.
                self.logger.info('Actually reading...')
                slices = tuple([slice(start_index[i], stop_index[i]) for i in range(data_variable.ndim)])
                data_slice = data_variable[slices]
                if lon_gap_mode:
                    self.logger.info('[Gap mode] Reading the second data part...')
                    slices_2 = tuple([slice(start_index_2[i], stop_index_2[i]) for i in range(data_variable.ndim)])
                    data_slice_2 = data_variable[slices_2]
                self.logger.info('Done!')

                if lon_gap_mode:
                    self.logger.info('[Gap mode] Swapping data and longitude grid...')
                    # Swap data parts.
                    data_slice = np.concatenate([data_slice_2, data_slice], axis=lon_index_pos)
                    # Swap longitude grid parts.
                    l_1 = lons[start_index[lon_index_pos]:stop_index[lon_index_pos]]
                    l_2 = lons[start_index_2[lon_index_pos]:stop_index_2[lon_index_pos]]
                    longitude_grid = np.concatenate([l_2, l_1])
                    self.logger.info('Done!')

                data_slice = np.squeeze(data_slice)  # Remove single-dimensional entries

                # Due to a very specific way of storing total precipitation in ERA Interim
                #  we fix values to reflect real precipitation accumulation during each time step.
                # Originally 6h-step data are stored as: tp@3h, tp@3h+tp@9h, tp@15h, tp@15h+tp@21h...
                #  We make them to be: tp@3h, tp@9h, tp@15h, tp@21h...
                # Originally 3h-step data are stored as: tp@3h, tp@3h+tp@6h, tp@3h+tp@6h+tp@9h, tp@15h, tp@15h+tp@18h, tp@15h+tp@18h+tp@21h...
                #  We make them to be: tp@3h, tp@6h, tp@9h, tp@15h, tp@18h, tp@21h... (note: there is no tp@12h in the time grid!)
                if self._data_info['@data_type'] == 'dataset':
                    if self._data_info['data']['dataset']['@name'].lower() == 'eraint' and \
                        self._data_info['data']['variable']['@name'].lower() == 'tp':
                        if self._data_info['data']['dataset']['@time_step'] == '6h':
                            for i in range(0, len(time_grid)-1, 2):
                                data_slice[i+1] -= data_slice[i]
                        elif self._data_info['data']['dataset']['@time_step'] == '3h':
                            for i in range(0, len(time_grid)-2, 2):
                                data_slice[i+2] -= data_slice[i+1]
                                data_slice[i+1] -= data_slice[i]
                        else:
                            self.logger.error('Error! Unsupported time step \'%s\'. Aborting...',
                                              self._data_info['data']['dataset']['@time_step'])
                            raise ValueError
                        # And, since negative values in total precipitation look weird (IMHO), let's fix them also.
                        data_slice[np.where(data_slice < 0)] = 0.0

                # Create masked array using ROI mask.
                self.logger.info('Creating masked array...')
                # TODO: Are we sure that the last two dimensions are lat and lon correspondingly?
                if data_slice.ndim > 2:  # When time dimension is present.
                    ROI_mask_time = np.tile(ROI_mask, (time_grid.size, 1, 1))  # Propagate ROI mask along the time dimension.
                else:                   # When there is only lat and lon dimensions are present.
                    ROI_mask_time = ROI_mask

                # Get/guess missing value from the data variable
                var_attrs_list = data_variable.ncattrs()
                if '_FillValue' in var_attrs_list:
                    fill_value = data_variable._FillValue     # pylint: disable=W0212
                elif 'missing_value' in var_attrs_list:
                    fill_value = data_variable.missing_value
                elif 'units' in var_attrs_list:
                    self.logger.info('No missing value attribute. Trying to guess...')
                    if data_slice.min() >= 0.0 - 1e12 and data_slice.min() <= 0.0 + 1e12 and data_variable.units == 'K':
                        fill_value = data_slice.min()
                        self.logger.info('Success! Set to %s', fill_value)
                    else:
                        self.logger.info('Can\'t guess missing value. Set to 1E20.')
                        fill_value = 1e20
                else:
                    self.logger.info('Can\'t get or guess missing value. Set to 1E20.')
                    fill_value = 1e20

                fill_value_mask = data_slice == fill_value
                combined_mask = ma.mask_or(fill_value_mask, ROI_mask_time, shrink=False)

                masked_data_slice = ma.MaskedArray(data_slice, mask=combined_mask, fill_value=fill_value)
                #self.logger.info('Min data value: %s, max data value: %s', masked_data_slice.min(), masked_data_slice.max())
                self.logger.info('Done!')

                # Apply scale/offset from the MDDB.
                self.logger.info('Applying scale/offset from the MDDB....')
                masked_data_slice = masked_data_slice * data_scale + data_offset
                self.logger.info('Done!')

                self._add_segment_data(level_name=level_name, values=masked_data_slice, time_grid=time_grid, time_segment=segment)

        # Remove level variable name from the list of data dimensions if it is present
        data_dim_names = list(dd)
        try:
            data_dim_names.remove(level_variable_name)
        except ValueError:
            pass

        # Get data description from metadata database for a dataset.
        if self._data_info['data']['@type'] == 'dataset':
            data_description = self._data_info['data']['description']
        # Get data description from a netcdf file metadata if not present.
        if self._data_info['data']['@type'] == 'raw' and 'description' not in self._data_info['data'].keys():
            data_description = {}
            data_description['@title'] = netcdf_root.Title
            data_description['@name'] = data_variable.long_name
            data_description['@units'] = data_variable.units
            data_description['@acc_mode'] = 0

        self._add_metadata(longitude_grid=longitude_grid, latitude_grid=latitude_grid, grid_type=grid_type, dimensions=data_dim_names,
                           description=data_description, fill_value=fill_value)

        self.logger.info('Done!')

        return self._get_result_data()

    def write(self, all_values, all_options):
        """Writes data array into a netCDF file.

        Arguments:
            all_values -- processing result's values as a list of masked array/array/list.
            all_options -- dictionary of lists of write options:
                ['level'] -- vertical level name
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
        """

        self.logger.info('Writing data to a netCDF file...')

        if all_values[0].ndim == 1:  # We have stations data.
            self.write_stations(all_values[0], all_options)  # There should be only one variable to write.
        else:
            self.write_array(all_values, all_options)

        self.logger.info('Done!')

    def write_array(self, all_values, all_options):
        """Writes data array into a netCDF file.

        Arguments:
            all_values -- processing result's values as a list of masked array/array/list.
            all_options -- dictionary of lists of write options:
                ['level'] -- vertical level name
                ['segment'] -- time segments descriptions (as in input time segments taken from a task file)
                ['times'] -- time grids for each segment as a list of lists of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as a masked array
                ['latitudes'] -- latitude grid (1-D or 2-D) as a masked array
                ['meta'] -- additional metadata as a dictionary
                ['description'] -- basic description of the data as a dictionary
        """

        filename = self._data_info['data']['file']['@name']
        self.logger.info('Writing netCDF file: %s', filename)

        # Get meta.
        meta = all_options.get('meta')

        # Get the variable name.
        varname = None if meta is None else all_options['meta'].get('varname')
        varname = 'data' if varname is None else varname

        # Get time values.
        time_grid = [item for sublist in all_options['times'] for item in sublist]
        if not time_grid:  # Time grids are not given.
            # Use segments to get the time grid.
            time_grid = [datetime.strptime(item['@beginning'], '%Y%m%d%H') for item in all_options['segment']]
        n_times = len(time_grid)

        # Get level values and names.
        levels = {}
        levels['values'] = []
        levels['units'] = set()
        for level in all_options['level']:
            level_value = re.findall(r'\d+', level)  # Take a numeric part.
            if level_value:
                levels['values'].append(int(level_value[0]))
            else:
                levels['values'].append(0)
            level_units = re.findall(r'[a-zA-Z]+', level)  # Take an alpha part.
            if level_units:
                levels['units'].add(level_units[0])

        if len(levels['units']) > 1:
            self.logger.error('Writing levels with different units are not supported yet! Aborting...')
            raise ValueError
        else:
            levels['units'] = levels['units'].pop() if levels['units'] else None
            n_levels = len(levels['values'])

        # Stack values.
        n_lat, n_lon = all_values[0].shape[-2:]
        values = ma.stack(all_values)
        values = values.reshape((n_levels, n_times, n_lat, n_lon))

        # Create netCDF file.
        try:
            root = Dataset(filename, 'w', clobber=False, format='NETCDF4_CLASSIC')  # , format='NETCDF3_64BIT_OFFSET')
            new_file = True
        except OSError:
            root = Dataset(filename, 'a')
            new_file = False

        if new_file:
            # Set global attributes.
            root.Title = all_options['description']['@title']
            root.Conventions = 'CF'
            root.Source = 'Generated by CLIMATE Web-GIS'

            # Define geographic variables.
            lon = root.createDimension('lon', all_options['longitudes'].size)  # pylint: disable=W0612
            longitudes = root.createVariable('lon', 'f4', ('lon'))
            lat = root.createDimension('lat', all_options['latitudes'].size)  # pylint: disable=W0612
            latitudes = root.createVariable('lat', 'f4', ('lat'))
            # Set geographic attributes.
            latitudes.standard_name = 'latitude'
            latitudes.units = 'degrees_north'
            latitudes.long_name = 'latitude'
            longitudes.standard_name = 'longitude'
            longitudes.units = 'degrees_east'
            longitudes.long_name = 'longitude'
            # Write geographic variables.
            longitudes[:] = all_options['longitudes']
            latitudes[:] = all_options['latitudes']

            # Define time variable.
            time_dim = root.createDimension('time', n_times)  # pylint: disable=W0612
            time_var = root.createVariable('time', 'f8', ('time'))
            # Set time attributes.
            time_var.units = 'days since {}-1-1 00:00:0.0'.format(time_grid[0].year)
            time_var_long_name = None if meta is None else meta.get('time_long_name')
            time_var.long_name = 'time' if time_var_long_name is None else time_var_long_name
            # Write time variable.
            start_date = datetime(time_grid[0].year, 1, 1)
            time_var[:] = [(cur_date - start_date).days for cur_date in time_grid]

            # Define level variable.
            level_dim = root.createDimension('level', n_levels)  # pylint: disable=W0612
            level_var = root.createVariable('level', 'i4', ('level'))
            # Set level attributes.
            level_var.standard_name = 'level'
            level_var_units = levels['units'] if levels['units'] else None if meta is None else meta.get('level_units')
            if level_var_units:
                level_var.units = level_var_units
            level_var_long_name = None if meta is None else meta.get('level_long_name')
            if level_var_long_name:
                level_var.long_name = level_var_long_name
            # Write level variable.
            level_var[:] = levels['values']

        # Check if variable is present in the file.
        data_var = root.variables.get(varname)
        if data_var is None:
            # Define data variable.
            data_dims = ['time', 'level', 'lat', 'lon']
            data_var = root.createVariable(varname, 'f4', data_dims, fill_value=values.fill_value)
            # Set data attributes.
            data_var.units = all_options['description']['@units']
            data_var.long_name = all_options['description']['@name']
        # Write data variable.
        for level_idx in range(n_levels):
            data_var[:, level_idx, :, :] = ma.filled(values[level_idx, :, :, :], fill_value=values.fill_value)  # Write values.

        root = None

        self.logger.info('Done!')

    def write_stations(self, values, options):
        """Writes stations data into a netCDF file.

        Arguments:
            all_values -- processing result's values as a list of masked array/array/list.
            options -- dictionary of write options:
                ['level'] -- vertical level name
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
        """

        self.logger.info(' Writing data to a netCDF file...')

        # Construct the file name
        # filename = make_raw_filename(self._data_info, options)
        filename = self._data_info['data']['file']['@name']

        # Create netCDF file.
        root = Dataset(filename, 'w', format='NETCDF4')  # , format='NETCDF3_64BIT_OFFSET')

        # Define dimensions.
        lon = root.createDimension('lon', options['longitudes'].size)  # pylint: disable=W0612
        lat = root.createDimension('lat', options['latitudes'].size)  # pylint: disable=W0612
        station = root.createDimension('station', options['meta']['stations']['@names'].size)  # pylint: disable=W0612

        # Get time values.
        time_grid = [item for sublist in options['times'] for item in sublist]
        if not time_grid:  # Time grids are not given.
            # Use segments to get the time grid.
            time_grid = [datetime.strptime(item['@beginning'], '%Y%m%d%H') for item in options['segment']]
        n_times = len(time_grid)

        if n_times > 1:
            # Define time variable.
            time_dim = root.createDimension('time', n_times)  # pylint: disable=W0612
            time_var = root.createVariable('time', 'f8', ('time'))
            # Set time attributes.
            time_var.units = 'days since {}-1-1 00:00:0.0'.format(time_grid[0].year)
            time_var_long_name = None if options['meta'] is None else options['meta'].get('time_long_name')
            time_var.long_name = 'time of measurement' if time_var_long_name is None else time_var_long_name
            start_date = datetime(time_grid[0].year, 1, 1)

        # Define variables.
        latitudes = root.createVariable('lat', 'f4', ('lat'))
        longitudes = root.createVariable('lon', 'f4', ('lon'))

        if n_times > 1:
            data = root.createVariable('data', 'f4', ('time', 'station'), fill_value=values.fill_value)
            coordinates = 'time lat lon alt'
        else:
            data = root.createVariable('data', 'f4', ('station'), fill_value=values.fill_value)
            coordinates = 'lat lon alt'
        station_name = root.createVariable('station_name', str, ('station'), fill_value=values.fill_value)
        wmo_code = root.createVariable('wmo_code', 'f4', ('station'), fill_value=values.fill_value)
        alt = root.createVariable('alt', 'f4', ('station'), fill_value=values.fill_value)

        # Set global attributes.
        root.Title = options['description']['@title']
        root.Conventions = 'CF'
        root.Source = 'Generated by CLIMATE Web-GIS'

        # Set variables attributes.
        latitudes.standard_name = 'latitude'
        latitudes.units = 'degrees_north'
        latitudes.long_name = 'station latitude'
        longitudes.standard_name = 'longitude'
        longitudes.units = 'degrees_east'
        longitudes.long_name = 'station longitude'
        alt.standard_name = 'height'
        alt.long_name = 'vertical distance above the surface'
        alt.units = 'm'
        alt.positive = 'up'
        alt.axis = 'Z'
        station_name.long_name = 'station name'
        data.coordinates = coordinates
        data.units = options['description']['@units']
        data.long_name = options['description']['@name']
        if n_times == 1:
            data.measurement_time = '{}-{}'.format(options['segment'][0]['@beginning'], options['segment'][0]['@ending'])

        # Write variables.
        if n_times > 1:
            time_var[:] = [(cur_date - start_date).days for cur_date in time_grid]
        longitudes[:] = options['longitudes']
        latitudes[:] = options['latitudes']
        data[:] = ma.filled(values, fill_value=values.fill_value)
        station_name[:] = options['meta']['stations']['@names']
        wmo_code[:] = options['meta']['stations']['@wmo_codes']
        alt[:] = options['meta']['stations']['@elevations']

        root = None

        self.logger.info('Done!')
