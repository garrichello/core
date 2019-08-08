"""Provides classes
    DataNetcdf
"""
from string import Template
from datetime import datetime

from copy import copy
from netCDF4 import MFDataset, date2index, num2date, Dataset, MFTime
import numpy as np
import numpy.ma as ma

from core.base.common import listify, unlistify, print, make_raw_filename  # pylint: disable=W0622
from .data import Data, GRID_TYPE_REGULAR

LONGITUDE_UNITS = {'degrees_east', 'degree_east', 'degrees_E', 'degree_E',
                   'degreesE', 'degreeE', 'lon'}
LATITUDE_UNITS = {'degrees_north', 'degree_north', 'degrees_N', 'degree_N',
                  'degreesN', 'degreeN', 'lat'}
TIME_UNITS = {'since', 'time'}
NO_LEVEL_NAME = 'none'
WILDCARDS = {'year': '????', 'mm': '??', 'year1': '????', 'year2': '????', 'year1s-4': '????', 'year2s-4': '????'}

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
        self._data_info = data_info
        super().__init__(data_info)

        self.file_name_wildcard = ''
        self.netcdf_root = None

    def __del__(self):
        if self.netcdf_root is not None:
            self.netcdf_root.close()

    def _get_longitudes(self, nc_root):
        longitude_variable = unlistify(nc_root.get_variables_by_attributes(units=lambda v: v in LONGITUDE_UNITS))
        if longitude_variable.ndim == 1:
            grid_type = GRID_TYPE_REGULAR
            lons = longitude_variable[:]
            if lons.max() > 180:
                lons = ((lons + 180.0) % 360.0) - 180.0  # Switch from 0-360 to -180-180 grid
        else:
            print(' (DataNetcdf::_get_longitudes) 2-D longitude grid is not implemented yet. Aborting...')
            raise ValueError

        return (lons, longitude_variable.name, grid_type)

    def _get_latitudes(self, nc_root):
        latitude_variable = unlistify(nc_root.get_variables_by_attributes(units=lambda v: v in LATITUDE_UNITS))
        if latitude_variable.ndim == 1:
            grid_type = GRID_TYPE_REGULAR
            lats = latitude_variable[:]
        else:
            print(' (DataNetcdf::_get_latitudes) 2-D latitude grid is not implemented yet. Aborting...')
            raise ValueError

        return (lats, latitude_variable.name, grid_type)

    def _get_levels(self, nc_root, level_name, level_variable_name):
        if level_variable_name != NO_LEVEL_NAME:
            try:
                level_variable = nc_root.variables[level_variable_name]  # pylint: disable=E1136
            except KeyError:
                print(' (DataNetcdf::read) Level variable \'{0}\' is not found in files. Aborting!'.format(
                    level_variable_name))
                raise
            try:
                # Little hack: convert level name from metadata database to float and back to string type
                # to be able to search it correctly in float type level variable converted to string.
                # Level variable is also primarily converted to float to 'synchronize' types.
                level_index = level_variable[:].astype('float').astype('str').tolist().index(str(float(level_name)))
            except KeyError:
                print(' (DataNetcdf::read) Level \'{0}\' is not found in level variable \'{1}\'. Aborting!'.format(
                    level_name, level_variable_name))
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

        print(' (DataNetcdf::read) Reading NetCDF data...')
        print(' (DataNetcdf::read) [Dataset: {}, resolution: {}, scenario: {}, time_step: {}]'.format(
            self._data_info['data']['dataset']['@name'], self._data_info['data']['dataset']['@resolution'],
            self._data_info['data']['dataset']['@scenario'], self._data_info['data']['dataset']['@time_step']
        ))

        # Levels must be a list or None.
        levels_to_read = listify(options['levels'])
        if levels_to_read is None:
            levels_to_read = self._data_info['levels']  # Read all levels if nothing specified.
        # Segments must be a list or None.
        segments_to_read = listify(options['segments'])
        if segments_to_read is None:
            segments_to_read = listify(self._data_info['data']['time']['segment'])  # Read all levels if nothing specified.

        variable_indices = {}  # Contains lists of indices for each dimension of the data variable in the domain to read.

        self._make_ROI()

        # Process each vertical level separately.
        for level_name in levels_to_read:
            print(' (DataNetcdf::read)  Vertical level: \'{0}\''.format(level_name))
            level_variable_name = self._data_info['data']['levels'][level_name]['@level_variable_name']

            data_scale = self._data_info['data']['levels'][level_name]['@scale']
            data_offset = self._data_info['data']['levels'][level_name]['@offset']

            file_name_template = self._data_info['data']['levels'][level_name]['@file_name_template']  # Template as in MDDB.
            percent_template = PercentTemplate(file_name_template)  # Custom string template %keyword%.
            file_name_wildcard = percent_template.substitute(WILDCARDS)  # Create wildcard-ed template

            # Kind of a caching for netcdf_root to save time working at the same vertical level.
            print(' (DataNetcdf::read)  Open files...')
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
            print(' (DataNetcdf::read)  Done!')

            data_variable = netcdf_root.variables[self._data_info['data']['variable']['@name']]  # Data variable. pylint: disable=E1136
            data_variable.set_auto_mask(False)
            dd = data_variable.dimensions  # Names of dimensions of the data variable.

            print(' (DataNetcdf::read)  Get grids...')
            # Determine indices of longitudes.
            lons, longitude_variable_name, lon_grid_type = self._get_longitudes(netcdf_root)
            longitude_indices = np.nonzero([ge and le for ge, le in
                                            zip(lons >= self._ROI_bounds['min_lon'], lons <= self._ROI_bounds['max_lon'])])[0]
            variable_indices[longitude_variable_name] = longitude_indices  # np.arange(lons.size)  # longitude_indices
            longitude_grid = lons[longitude_indices]

            # Determine indices of latitudes.
            lats, latitude_variable_name, lat_grid_type = self._get_latitudes(netcdf_root)
            latitude_indices = np.nonzero([ge and le for ge, le in
                                           zip(lats >= self._ROI_bounds['min_lat'], lats <= self._ROI_bounds['max_lat'])])[0]
            variable_indices[latitude_variable_name] = latitude_indices  # np.arange(lats.size)  # latitude_indices
            latitude_grid = lats[latitude_indices]

            if lon_grid_type == lat_grid_type:
                grid_type = lon_grid_type
            else:
                print(' (DataNetcdf::read)  Error! Longitude and latitude grids are not match! Aborting.')
                raise ValueError

            # Determine index of the current vertical level to read data variable.
            level_index = self._get_levels(netcdf_root, level_name, level_variable_name)
            if level_index is not None:
                variable_indices[level_variable_name] = [level_index]

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
            time_variable = MFTime(time_variable, calendar=calendar)  # Apply multi-file support to the time variable

            print(' (DataNetcdf::read)  Done!')

            # Create ROI mask.
            ROI_mask = self._make_ROI_mask(longitude_grid, latitude_grid)

            # Process each time segment separately.
            self._init_segment_data(level_name)  # Initialize a data dictionary for the vertical level 'level_name'.
            for segment in segments_to_read:
                print(' (DataNetcdf::read)  Time segment \'{0}\''.format(segment['@name']))

                segment_start = datetime.strptime(segment['@beginning'], '%Y%m%d%H')
                segment_end = datetime.strptime(segment['@ending'], '%Y%m%d%H')
                time_idx_range = date2index([segment_start, segment_end], time_variable, select='nearest')
                if time_idx_range[1] == 0:
                    print(''' (DataNetcdf::read)  Error! The end of the time segment is before the first time in the dataset.
                                           Aborting!''')
                    raise ValueError
                variable_indices[time_variable._name] = np.arange(time_idx_range[0], time_idx_range[1])  # pylint: disable=W0212, E1101
                time_values = time_variable[variable_indices[time_variable._name]]  # Raw time values.  # pylint: disable=W0212, E1101
                time_grid = num2date(time_values, time_variable.units)  # Time grid as a datetime object.  # pylint: disable=E1101

                # Searching for a gap in longitude indices. Normally all steps should be equal to 1.
                # If there is a step longer than 1, we suupose it's a gap due to a shift from 0-360 to -180-180 grid.
                # So instead of a sigle patch in a 0-360 longitude space we should deal with two patches
                # in a -180-180 longitude space: one to the left of the 0 meridian, and the other to the right of it.
                # So we search for the gap's position and prepare to read two parts of data.
                # Then we stack them reversely (left to the right) and correct longitude grid.
                # Thus we will have a single data patch with the uniform logitude grid.
                lon_index_steps = np.diff(variable_indices[longitude_variable_name])  # Steps between indices.
                if lon_index_steps.max() > 1:  # Gap is a step longer than 1 (there should be only one gap).
                    lon_gap_mode = True  # Gap mode! Set the flag! :)
                    lon_gap_position = lon_index_steps.argmax()  # Index of the gap.
                else:
                    lon_gap_mode = False  # Normal mode.

                # Get start (first) and stop (last) indices for each dimension.
                start_index = [variable_indices[dd[i]][0] for i in range(len(dd))]
                stop_index = [variable_indices[dd[i]][-1]+1 for i in range(len(dd))]
                if lon_gap_mode:  # For gap mode we need start and stop indices for the second data part.
                    print(' (DataNetcdf::read)  Longitude gap detected. Gap mpde activated!')
                    lon_index_pos = dd.index(longitude_variable_name)  # Position of the longitude indices in dimensions list.
                    start_index_2 = copy(start_index)
                    stop_index_2 = copy(stop_index)
                    stop_index[lon_index_pos] = \
                        variable_indices[longitude_variable_name][lon_gap_position]+1  # First part ends here.
                    start_index_2[lon_index_pos] = \
                        variable_indices[longitude_variable_name][lon_gap_position+1]  # Second part starts here.

                # Here we actually read the data array from the file for all lons and lats (it's faster to read everything).
                # And mask all points outside the ROI mask for all times.
                print(' (DataNetcdf::read)  Actually reading...')
                slices = [slice(start_index[i], stop_index[i]) for i in range(data_variable.ndim)]
                data_slice = data_variable[slices]
                if lon_gap_mode:
                    print(' (DataNetcdf::read)  [Gap mode] Reading the second data part...')
                    slices_2 = [slice(start_index_2[i], stop_index_2[i]) for i in range(data_variable.ndim)]
                    data_slice_2 = data_variable[slices_2]
                print(' (DataNetcdf::read)  Done!')

                if lon_gap_mode:
                    print(' (DataNetcdf::read)  [Gap mode] Swapping data and longitude grid...')
                    # Swap data parts.
                    data_slice = np.concatenate([data_slice_2, data_slice], axis=lon_index_pos)
                    # Swap longitude grid parts.
                    l_1 = lons[start_index[lon_index_pos]:stop_index[lon_index_pos]]
                    l_2 = lons[start_index_2[lon_index_pos]:stop_index_2[lon_index_pos]]
                    longitude_grid = np.concatenate([l_2, l_1])
                    print(' (DataNetcdf::read)  Done!')

                data_slice = np.squeeze(data_slice)  # Remove single-dimensional entries

                # Create masked array using ROI mask.
                print(' (DataNetcdf::read)  Creating masked array...')
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
                    print(' (DataNetcdf::read)  No missing value attribute. Trying to guess...')
                    if data_slice.min() >= 0.0 - 1e12 and data_slice.min() <= 0.0 + 1e12 and data_variable.units == 'K':
                        fill_value = data_slice.min()
                        print(' (DataNetcdf::read)   Success! Set to {}'.format(fill_value))
                    else:
                        print(' (DataNetcdf::read)   Can\'t guess missing value. Set to 1E20.')
                        fill_value = 1e20
                else:
                    print(' (DataNetcdf::read)  Can\'t get or guess missing value. Set to 1E20.')
                    fill_value = 1e20

                fill_value_mask = data_slice == fill_value
                combined_mask = ma.mask_or(fill_value_mask, ROI_mask_time, shrink=False)

                masked_data_slice = ma.MaskedArray(data_slice, mask=combined_mask, fill_value=fill_value)
                #print('(DataNetcdf::read)   Min data value: {}, max data value: {}'.format(masked_data_slice.min(), masked_data_slice.max()))
                print(' (DataNetcdf::read)  Done!')

                # Apply scale/offset from the MDDB.
                print(' (DataNetcdf::read)  Applying scale/offset from the MDDB....')
                masked_data_slice = masked_data_slice * data_scale + data_offset
                print(' (DataNetcdf::read)  Done!')

                self._add_segment_data(level_name=level_name, values=masked_data_slice, time_grid=time_grid, time_segment=segment)

        # Remove level variable name from the list of data dimensions if it is present
        data_dim_names = list(dd)
        try:
            data_dim_names.remove(level_variable_name)
        except ValueError:
            pass

        self._add_metadata(longitude_grid=longitude_grid, latitude_grid=latitude_grid, grid_type=grid_type, dimensions=data_dim_names,
                           description=self._data_info['data']['description'], fill_value=fill_value)

        print(' (DataNetcdf::read) Done!')

        return self._get_result_data()

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

        print(' (DataNetcdf::write) Writing data to a netCDF file...')

        if values.ndim == 1:  # We have stations data.
            self.write_stations(values, options)
        else:
            self.write_array(values, options)

        print(' (DataNetcdf::write) Done!')

    def write_array(self, values, options):
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

        # Construct the file name
        filename = make_raw_filename(self._data_info, options)

        print(' (DataNetcdf::write_array)  Writing netCDF file: {}'.format(filename))

        # Create netCDF file.
        try:
            root = Dataset(filename, 'w', clobber=False)  # , format='NETCDF3_64BIT_OFFSET')
            NEW_FILE = True
        except OSError:
            root = Dataset(filename, 'a')
            NEW_FILE = False

        # Define dimensions and variables.
        if NEW_FILE:
            lon = root.createDimension('lon', options['longitudes'].size)  # pylint: disable=W0612
            longitudes = root.createVariable('lon', 'f4', ('lon'))
            lat = root.createDimension('lat', options['latitudes'].size)  # pylint: disable=W0612
            latitudes = root.createVariable('lat', 'f4', ('lat'))
            lev = root.createDimension('level')  # pylint: disable=W0612
            levels = root.createVariable('level', str, ('level'))

            if options['times'] is not None:
                time = root.createDimension('time', len(options['times']))  # pylint: disable=W0612
                times = root.createVariable('time', 'f8', ('time'))
                times.units = 'days since {}-1-1 00:00:0.0'.format(options['times'][0].year)
                times_long_name = options['meta'].get('time_long_name')
                times.long_name = 'time' if times_long_name is None else times_long_name

        # Define data variable
        varname = options['meta'].get('varname')
        varname = 'data' if varname is None else varname
        if options['times'] is not None:
            if varname not in root.variables:
                data = root.createVariable(varname, 'f4', ('time', 'level', 'lat', 'lon'), fill_value=values.fill_value)
            else:
                data = root.variables[varname]
            values = values.reshape((len(options['times']), 1, options['latitudes'].size, options['longitudes'].size))
        else:
            if varname not in root.variables:
                data = root.createVariable(varname, 'f4', ('level', 'lat', 'lon'), fill_value=values.fill_value)
            else:
                data = root.variables[varname]
            values = values.reshape((1, options['latitudes'].size, options['longitudes'].size))

        # Set global attributes.
        if NEW_FILE:
            root.Title = options['description']['@title']
            root.Conventions = 'CF'
            root.Source = 'Generated by CLIMATE Web-GIS'

        # Set variables attributes.
        if NEW_FILE:
            levels.standard_name = 'level'
            levels.units = 'string'
            levels.long_name = 'level'
            latitudes.standard_name = 'latitude'
            latitudes.units = 'degrees_north'
            latitudes.long_name = 'latitude'
            longitudes.standard_name = 'longitude'
            longitudes.units = 'degrees_east'
            longitudes.long_name = 'longitude'
        data.units = options['description']['@units']
        data.long_name = '{} {}'.format(options['description']['@title'], options['description']['@name'])

        # Write variables.
        # TODO: May be in the future we will write time grid into a file. If needed.
        if NEW_FILE:
            if options['times'] is not None:
                start_date = datetime(options['times'][0].year, 1, 1)
                times[:] = [(cur_date - start_date).days for cur_date in options['times']]
            longitudes[:] = options['longitudes']
            latitudes[:] = options['latitudes']

        new_level_idx = levels.size+1
        levels[new_level_idx] = options['level']
        data[:, new_level_idx, :, :] = ma.filled(values, fill_value=values.fill_value)

        root = None

        print(' (DataNetcdf::write_array)  Done!')

    def write_stations(self, values, options):
        """Writes stations data into a netCDF file.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options:
                ['level'] -- vertical level name
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
        """

        print('(DataNetcdf::write_stations)  Writing data to a netCDF file...')

        # Construct the file name
        filename = make_raw_filename(self._data_info, options)

        # Create netCDF file.
        root = Dataset(filename, 'w')  # , format='NETCDF3_64BIT_OFFSET')

        # Define dimensions.
        lon = root.createDimension('lon', options['longitudes'].size)  # pylint: disable=W0612
        lat = root.createDimension('lat', options['latitudes'].size)  # pylint: disable=W0612
        station = root.createDimension('station', options['meta']['stations']['@names'].size)  # pylint: disable=W0612
        times_long_name = 'time of measurement'

        if options['times'] is not None:
            time = root.createDimension('time', options['times'].size)  # pylint: disable=W0612
            times = root.createVariable('time', 'f8', ('time'))
            times.units = 'days since 1970-1-1 00:00:0.0'
            times.long_name = times_long_name

        # Define variables.
        latitudes = root.createVariable('lat', 'f4', ('lat'))
        longitudes = root.createVariable('lon', 'f4', ('lon'))

        if options['times'] is not None:
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
        data.long_name = '{} {}'.format(options['description']['@title'], options['description']['@name'])

        # Write variables.
        # TODO: May be in the future we will write time grid into a file. If needed.
        if options['times'] is not None:
            pass

        longitudes[:] = options['longitudes']
        latitudes[:] = options['latitudes']
        data[:] = ma.filled(values, fill_value=values.fill_value)
        station_name[:] = options['meta']['stations']['@names']
        wmo_code[:] = options['meta']['stations']['@wmo_codes']
        alt[:] = options['meta']['stations']['@elevations']

        root = None

        print(' (DataNetcdf::write_stations)  Done!')
