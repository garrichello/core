"""Provides classes
    DataHdfeos
"""

from datetime import datetime
import re
import numpy as np
import numpy.ma as ma
from core.base.common import listify, print  # , make_filename
from .data import Data, GRID_TYPE_REGULAR
from .mfhdf import MFDataset, date2index

NO_LEVEL_NAME = 'none'
CLASS_UNITS = ['class number']


class DataHdfeos(Data):
    """ Provides methods for reading and writing archives of HDF4 files.
    """
    def __init__(self, data_info):
        self._data_info = data_info
        super().__init__(data_info)

    def read(self, options):
        """Reads HDF-EOS file into an array.

        Arguments:
            options -- dictionary of read options:
                ['segments'] -- time segments
                ['levels'] -- vertical levels

        Returns:
            result['array'] -- data array
        """

        print('(DataHdfeos::read) Reading HDF-EOS data...')

        # Levels must be a list or None.
        levels_to_read = listify(options['levels'])
        if levels_to_read is None:
            levels_to_read = self._data_info['levels']  # Read all levels if nothing specified.
        # Segments must be a list or None.
        segments_to_read = listify(options['segments'])
        if segments_to_read is None:
            segments_to_read = listify(self._data_info['data']['time']['segment'])  # Read all levels if nothing specified.

        variable_indices = {}  # Contains lists of indices for each dimension of the data variable in the domain to read.

        # Process each vertical level separately.
        for level_name in levels_to_read:
            print('(DataHdfeos::read)  Reading level: \'{0}\''.format(level_name))
            file_name_template = self._data_info['levels'][level_name]['@file_name_template']  # Template as in MDDB.
            # Create wildcard-ed template.
            file_name_template = re.sub(r'\%[a-z0-9\-]{2}\%', '??', file_name_template)  # Replace %mm% and %dd% with ??.
            file_name_template = re.sub(r'\%[a-z0-9\-]{3}\%', '???', file_name_template)  # Replace %doy% with ???.
            file_name_wildcard = re.sub(r'\%[a-z0-9\-]*\%', '????', file_name_template)  # Replace %year...% with ????.

            hdf_root = MFDataset(file_name_wildcard)

            data_variable = hdf_root.variables[self._data_info['data']['variable']['@name']]  # Data variable.

            # Get level variable.
            level_variable = data_variable.get_level_variable()
            level_variable_name = data_variable.get_level_variable_name()

            # Determine indices of longitudes.
            longitude_variable = hdf_root.get_longitude_variable()
            if longitude_variable.ndim == 1:
                lon_grid_type = GRID_TYPE_REGULAR
                lons = longitude_variable[:]
                if lons.max() > 180:
                    lons = ((lons + 180.0) % 360.0) - 180.0  # Switch from 0-360 to -180-180 grid
            variable_indices['XDim'] = np.arange(lons.size)  # longitude_indices

            # Determine indices of latitudes.
            latitude_variable = hdf_root.get_latitude_variable()
            if latitude_variable.ndim == 1:
                lat_grid_type = GRID_TYPE_REGULAR
                lats = latitude_variable[:]
            variable_indices['YDim'] = np.arange(lats.size)  # latitude_indices

            if lon_grid_type == lat_grid_type:
                grid_type = lon_grid_type
            else:
                print('(DataHdfeos::read) Error! Longitude and latitude grids are not match! Aborting.')
                raise ValueError

            # Create ROI mask.
            ROI_mask = self._create_ROI_mask(lons, lats)

            # Determine index of the current vertical level to read data variable.
            if level_variable_name is not None:
                variable_indices[level_variable_name] = [level_variable.tolist().index(level_name)]
            else:
                level_variable_name = NO_LEVEL_NAME

            # Get time variable
            time_variable = hdf_root.get_time_variable()

            # Process each time segment separately.
            self._init_segment_data(level_name)  # Initialize a data dictionary for the vertical level 'level_name'.
            for segment in segments_to_read:
                print('(DataHdfeos::read)  Reading time segment \'{0}\''.format(segment['@name']))

                segment_start = datetime.strptime(segment['@beginning'], '%Y%m%d%H')
                segment_end = datetime.strptime(segment['@ending'], '%Y%m%d%H')
                time_idx_range = date2index([segment_start, segment_end], time_variable)
                if time_idx_range[1] == 0:
                    print('''(DataHdfeos::read) Error! The end of the time segment is before the first time in the dataset.
                            Aborting!''')
                    raise ValueError
                variable_indices['Time'] = np.arange(time_idx_range[0], time_idx_range[1] + 1)
                time_grid = time_variable[variable_indices['Time']]  # Time grid.

                dd = data_variable.dimensions  # Names of dimensions of the data variable.

                # Here we actually read the data array from the file for all lons and lats (it's faster to read everything).
                # And mask all points outside the ROI mask for all times.
                print('(DataHdfeos::read)  Actually reading...')
                if data_variable.ndim == 4:
                    data_slice = data_variable[variable_indices[dd[0]], variable_indices[dd[1]],
                                               variable_indices[dd[2]], variable_indices[dd[3]]]
                if data_variable.ndim == 3:
                    data_slice = data_variable[variable_indices[dd[0]], variable_indices[dd[1]],
                                               variable_indices[dd[2]]]
                print('(DataHdfeos::read)  Done!')

                if data_slice.shape[-1] == 1:   # we expect last two dimensions are lat and lon
                    data_slice = data_slice[:, :, :, 0] 
                #data_slice = np.squeeze(data_slice)  # Remove single-dimensional entries

                # Create masks.
                ROI_mask_time = ROI_mask
                fill_value = data_variable._FillValue   # pylint: disable=W0212
                fill_value_mask = data_slice == fill_value
                combined_mask = ma.mask_or(fill_value_mask, ROI_mask_time)

                # Create masked array using ROI mask.
                print('(DataHdfeos::read)  Creating masked array...')
                masked_data_slice = ma.MaskedArray(data_slice, mask=combined_mask, fill_value=fill_value)
                print('(DataHdfeos::read)   Min data value: {}, max data value: {}'.format(masked_data_slice.min(), masked_data_slice.max()))
                print('(DataHdfeos::read)  Done!')

                # Remove level variable name from the list of data dimensions if it is present
                data_dim_names = list(dd)
                if level_variable_name != NO_LEVEL_NAME:
                    data_dim_names.remove(level_variable_name)

                self._add_segment_data(level_name=level_name, values=masked_data_slice,
                                       description=self._data_info['data']['description'],
                                       dimensions=data_dim_names, time_grid=time_grid, time_segment=segment)

        # If data variable units are class numbers, generate meta dictionary containing levels names for the legend.
        if data_variable.units in CLASS_UNITS:
            meta = {}
            meta['levels'] = {i: level for i, level in enumerate(level_variable)}
        else:
            meta = None

        self._add_metadata(longitude_grid=lons, latitude_grid=lats, grid_type=grid_type, fill_value=fill_value, meta=meta)

        print('(DataHdfeos::read) Done!')

        return self._get_result_data()

    def write(self, values, options):
        """Writes data array into a HDF-EOS file.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options:
                ['level'] -- vertical level name
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
        """

        print('(DataHdfeos::write) Writing data to a HDF-EOS file...')
        print(values)
        print(options)
        print('(DataHdfeos::write) Done!')
        raise NotImplementedError
