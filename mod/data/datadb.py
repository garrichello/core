"""Provides classes
    DataDB
"""
from sqlalchemy import create_engine, Table, MetaData, func, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from geoalchemy2 import *
from datetime import datetime
import numpy as np

from base.common import listify, unlistify, print

class DataDb:
    """ Provides methods for reading and writing geodatabase files.
    """
    def __init__(self, data_info):
        self._data_info = data_info

    def construct_polygon(self):
        """ Constructs a string defining a PostGIS POLYGON element
        """

        ROI_polygon = 'POLYGON(('
        for ROI_point in self._data_info['data']['region']['point']:
                ROI_polygon += '{} {}, '.format(ROI_point['@lon'], ROI_point['@lat'])
        ROI_polygon += '{} {}))'.format(self._data_info['data']['region']['point'][0]['@lon'], 
            self._data_info['data']['region']['point'][0]['@lat'])
        return ROI_polygon

    def read(self, options):
        """Queries database for in-situ measurements and puts them into an array.

        Arguments:
            options -- dictionary of read options:
                ['segments'] -- time segments
                ['levels'] -- vertical levels

        Returns:
            result['array'] -- data array
        """

        # Set default fill value here
        fill_value = -999

        # Variable name to query
        variable_name = self._data_info['data']['variable']['@name']

        # Levels must be a list or None.
        levels_to_read = listify(options['levels'])
        if levels_to_read is None:
            levels_to_read = self._data_info['levels']  # Read all levels if nothing specified.
        # Segments must be a list or None.
        segments_to_read = listify(options['segments'])
        if segments_to_read is None:
            segments_to_read = listify(self._data_info['data']['time']['segment'])  # Read all levels if nothing specified.

        # ROI as a POLYGON.
        ROI_polygon = self.construct_polygon()

        result = {} # Contains data arrays, grids and some additional information.
        result['data'] = {} # Contains data arrays being read from netCDF files at each vertical level.

        # Process each vertical level separately.
        for level_name in levels_to_read:
            print ('(DataDb::read) Reading level: \'{0}\''.format(level_name))
            file_name_template = self._data_info['levels'][level_name]['@file_name_template']
            (db_name, scenario, hor_res, time_step, tables_names) = file_name_template.split('/')
            db_url = 'postgresql://{}'.format(db_name.replace(':', '/'))
            engine = create_engine(db_url)
            meta = MetaData(bind = engine, reflect = True)
            Session = sessionmaker(bind=engine)
            session = Session()
        
            # Get tables objects
            stations_tbl = meta.tables['stations']
            st_data_tbl = meta.tables['st_data']

            # Process each time segment separately.
            data_by_segment = {} # Contains data array for each time segment.
            for segment in segments_to_read:
                print ('(DataNetcdf::read) Reading time segment \'{0}\''.format(segment['@name']))

                # Date is stored in the PostGIS DB as integers of form YYYYMMDD.
                # So convert string dates into integers and cut off hours.
                date_start = int(segment['@beginning'])//100
                date_end = int(segment['@ending'])//100

                # Form query
                q = session.query(st_data_tbl.columns[variable_name], st_data_tbl.columns.date, st_data_tbl.columns.station, 
                    stations_tbl.columns.st_name, func.ST_AsText(stations_tbl.columns.location).label('location')).join(
                    stations_tbl, st_data_tbl.columns.station == stations_tbl.columns.station).filter(
                    func.ST_Covers(ROI_polygon, stations_tbl.columns.location.ST_AsText())).filter(
                    and_(st_data_tbl.columns.date >= date_start, st_data_tbl.columns.date <= date_end))

                res = q.all()
                q = None

                all_stations_codes = np.array([int(st.station) for st in res])
                stations_codes = list(set(all_stations_codes))

                # Create time grid. It is the same for all stations now. So we take the first station as a source.
                station_indices = np.where(all_stations_codes == stations_codes[0])[0]
                time_grid = [datetime.strptime(str(res[i].date), '%Y%m%d') for i in station_indices]
                
                values = None
                longitudes = []
                latitudes = []
                elevations = []
                stations_names = []
                for station_code in stations_codes:
                    # Select rows in the query response corresponding to a station WMO code
                    station_indices = np.where(all_stations_codes == station_code)[0]
                    # 0-th element should always reference to data values.
                    station_values = np.ma.MaskedArray([res[i][0] for i in station_indices])
                    values = station_values if values is None else np.vstack((values, station_values))
                    # Station location is taken from the first row in the query response corresponding this station.
                    station_location = res[station_indices[0]].location.replace('(', '').replace(')', '').split(' ')
                    longitudes.append(float(station_location[2]))
                    latitudes.append(float(station_location[3]))
                    elevations.append(float(station_location[4]))
                    stations_names.append(res[station_indices[0]].st_name)

                values = values.transpose()
                values.fill_value = fill_value
                mask = values == fill_value
                values.mask = mask
                data_by_segment[segment['@name']] = {}
                data_by_segment[segment['@name']]['@values'] = values
                data_by_segment[segment['@name']]['description'] = self._data_info['data']['description']
                data_by_segment[segment['@name']]['@dimensions'] = ('time', 'station')
                data_by_segment[segment['@name']]['@time_grid'] = time_grid
                data_by_segment[segment['@name']]['segment'] = segment
            
            result['data'][level_name] = data_by_segment
            result['@longitude_grid'] = np.array(longitudes)
            result['@latitude_grid'] = np.array(latitudes)
            result['@grid_type'] = 'station'
            result['@fill_value'] = fill_value
            result['meta'] = {}
            result['meta']['stations'] = {}
            result['meta']['stations']['@names'] = np.array(stations_names)
            result['meta']['stations']['@wmo_codes'] = np.array(stations_codes)
            result['meta']['stations']['@elevations'] = np.array(elevations)

            session.close()
            

        return result


    def write(self, values, options):
        """Writes data array into a database tables.

        Arguments:
            values -- processing result's values as a masked array/array/list.
            options -- dictionary of write options:
                ['level'] -- vertical level name 
                ['segment'] -- time segment description (as in input time segments taken from a task file)
                ['times'] -- time grid as a list of datatime values
                ['longitudes'] -- longitude grid (1-D or 2-D) as an array/list
                ['latitudes'] -- latitude grid (1-D or 2-D) as an array/list
        """    
        
        print('(DataDB::write) Writing DB...')
        print('(DataDB::write) Not implemented yet!')
        pass