"""Provides classes:
    DataAccess
"""

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from .common import load_module, make_module_name, listify, print  # pylint: disable=W0622

ENGLISH_LANG_CODE = '409'

class DataAccess():
    """Class-helper for accessing data.
    Provides access to data through unified API for processing modules.

    External API:
        ::get(uid, segments=None, levels=None):
        Reads data and metadata from an input data source (dataset, parameter, array).
            uid -- processing module input's UID (as in a task file)
            segments -- list of time segments (read all if omitted)
            levels - list of vertical level (read all if omitted)

        ::input_uids():
        Returns a list of UIDs of processing module inputs (as in a task file)

        ::get_segments(uid):
        Returns time segments list
            uid -- UID of a processing module's input (as in a task file)

        ::get_levels(uid):
        Returns vertical levels list
            uid -- UID of a processing module's input  (as in a task file)

        ::put(uid, values, level=None, segment=None, times=None, longitudes=None, latitudes=None, fill_value=None,
                description = None, meta = None):
        Writes data and metadata to an output data storage (array).
            uid -- UID of a processing module's output (as in a task file)
            values -- processing result's values as a masked array/array/list
            level -- vertical level name segment -- time segment description (as in input time segments taken from a task file)
            times -- time grid as a list of datatime values
            longitudes -- longitude grid (1-D or 2-D) as an array/list
            latitudes -- latitude grid (1-D or 2-D) as an array/list
            fill_value -- fill value
            description -- dictionary describing data:
                ['title'] -- general title of the data (e.g., Average)
                ['name'] --  name of the data (e.g., Temperature)
                ['units'] -- units of the data (e.g., K)
            meta -- additional metadata passed from data readers to data writers through data processors

        ::output_uids():
        Returns a list of UIDs of processing module outputs (as in a task file)
    """

    def __init__(self, inputs, outputs, metadb_info):
        """Initializes class's attributes. Reads metadata database.
        Instantiate classes-readers and classes-writers for input and output arguments of a processing module
        correspondingly.

        Arguments:
            inputs -- list of dictionaries describing input arguments of a processing module
            outputs -- list of dictionaries describing output arguments of a processing module
            metadb_info -- dictionary describing metadata database (location and user credentials)
        """

        self._input_uids = []  # UIDs of input data sources.
        self._output_uids = []  # UID of output data destinations.
        self._data_objects = {}  # Instanses of data access classes.
        self._data_types = {}   # Types of datasets.

        # Process input arguments: None - no inputs; get metadata for each data source (if any)
        # and instantiate corresponding classes.
        print('(DataAccess::__init__) Prepare inputs...')
        self._inputs = listify(inputs)
        if self._inputs is None:
            self._input_uids = None
        else:
            for input_ in self._inputs:
                uid = input_['@uid']
                self._input_uids.append(uid)
                # Get additional info about an input from the metadata database
                input_info = self._get_metadata(metadb_info, input_)
                #  Data access class name is: 'Data' + <data type name> (e.g., DataNetcdf)
                data_class_name = 'Data' + input_info['@data_type'].capitalize()
                print('(DataAccess::__init__)  Input data module: {}'.format(data_class_name))
                module_name = make_module_name(data_class_name)
                data_class = load_module(module_name, data_class_name, package_name=self.__module__)
                if input_['data'].get('@object') is None:
                    input_['data']['@object'] = data_class(input_info)  # Try to instantiate data reading class
                self._data_objects[uid] = input_['data']['@object']
                self._data_types[uid] = input_info['@data_type']

        print('(DataAccess::__init__) Done!')

        # Process ouput argumetns: None - no outputs; get metadata for each data destination (if any)
        # and instantiate corresponding classes.
        print('(DataAccess::__init__) Prepare outputs...')
        self._outputs = listify(outputs)
        if outputs is None:
            self._output_uids = None
        else:
            for output_ in self._outputs:
                uid = output_['@uid']
                self._output_uids.append(uid)
                # Get additional info about an output from the metadata database
                output_info = self._get_metadata(metadb_info, output_)
                #  Data access class name is: "Data" + <File type name> (e.g., DataNetcdf)
                data_class_name = 'Data' + output_info['@data_type'].capitalize()
                print('(DataAccess::__init__)  Output data module: {}'.format(data_class_name))
                module_name = make_module_name(data_class_name)
                data_class = load_module(module_name, data_class_name, package_name=self.__module__)
                if output_['data'].get('@object') is None:
                    output_['data']['@object'] = data_class(output_info)  # Try to instantiate data writing class
                self._data_objects[uid] = output_['data']['@object']
                self._data_types[uid] = output_info['@data_type']

        print('(DataAccess::__init__) Done!')

        self._metadb = metadb_info

    def _get_metadata(self, metadb_info, argument):
        """Loads metadata from metadata database for an argument (input or output of the processing module)

        Arguments:
            metadb_info -- dictionary containing information about metadata database.
            argument -- dictionary containing description of the processing module's argument

        Returns: dictionary containing metadata for the 'argument':
            ["@data_type"] -- file type for a dataset (e.g., netcdf), data type otherwise (e.g.: parameter, array).

        """
        info = {} # Argument's information

        # We don't need to do much if it is not a dataset
        if argument['data']['@type'] != 'dataset':
            info['@data_type'] = argument['data']['@type']
            info['data'] = argument['data']
            return info

        # If it is a dataset there is much to do
        info['data'] = argument['data'] # All the information about the dataset is passed to data-accessing modules
        
        # Metadata database URL
        db_url = 'mysql://{0}@{1}/{2}'.format(metadb_info['@user'], metadb_info['@host'], metadb_info['@name'])
        engine = create_engine(db_url)
        meta = MetaData(bind=engine, reflect=True)
        session_class = sessionmaker(bind=engine)
        session = session_class()

        # Tables in a metadata database
        collection_tbl = meta.tables['collection']
        collection_tr_tbl = meta.tables['collection_tr']
        scenario_tbl = meta.tables['scenario']
        resolution_tbl = meta.tables['res']
        time_step_tbl = meta.tables['tstep']
        dataset_tbl = meta.tables['ds']
        file_type_tbl = meta.tables['filetype']
        data_tbl = meta.tables['data']
        file_tbl = meta.tables['file']
        variable_tbl = meta.tables['var']
        levels_tbl = meta.tables['lvs']
        levels_variable_tbl = meta.tables['lvs_var']
        dataset_root_tbl = meta.tables['dsroot']
        time_span_tbl = meta.tables['timespan']
        units_tbl = meta.tables['units']
        units_tr_tbl = meta.tables['units_tr']
        par_tbl = meta.tables['par']
        par_tr_tbl = meta.tables['par_tr']

        # Values for SQL-conditions
        dataset_name = argument['data']['dataset']['@name']
        scenario_name = argument['data']['dataset']['@scenario']
        resolution_name = argument['data']['dataset']['@resolution']
        time_step_name = argument['data']['dataset']['@time_step']
        variable_name = argument['data']['variable']['@name']
        levels_names = [level_name.strip() for level_name in argument['data']['levels']['@values'].split(';')]

        # Get some info about the dataset.
        try:
            dataset_tbl_info = \
                session.query(dataset_tbl.columns['id'],
                              file_type_tbl.columns['name'].label('file_type_name'),
                              scenario_tbl.columns['subpath0'],
                              resolution_tbl.columns['subpath1'],
                              time_step_tbl.columns['subpath2'],
                              time_span_tbl.columns['name'].label('file_time_span'),
                              collection_tr_tbl.columns['name'].label('collection_name'),
                              dataset_root_tbl.columns['rootpath']).select_from(dataset_tbl).join(
                                  collection_tbl).join(collection_tr_tbl).join(scenario_tbl).join(resolution_tbl).join(
                                      time_step_tbl).join(file_type_tbl).join(dataset_root_tbl).join(time_span_tbl).filter(
                                          collection_tbl.columns['name'] == dataset_name).filter(
                                              scenario_tbl.columns['name'] == scenario_name).filter(
                                                  resolution_tbl.columns['name'] == resolution_name).filter(
                                                      time_step_tbl.columns['name'] == time_step_name).filter(
                                                          collection_tr_tbl.columns['lang_code'] == ENGLISH_LANG_CODE).one()
        except NoResultFound:
            print('{} collection: {}, scenario: {}, resolution: {}, time step: {}'.format(
                '(DataAccess::_get_metadata) No records found in MDDB for', dataset_name, scenario_name,
                resolution_name, time_step_name))
            raise

        info['@data_type'] = dataset_tbl_info.file_type_name
        info['@file_time_span'] = dataset_tbl_info.file_time_span

        # Get units and full name of the variable
        try:
            var_tbl_info = \
                session.query(units_tr_tbl.columns['name'].label('units_name'), 
                              par_tr_tbl.columns['name'].label('parameter_name')).select_from(
                                  variable_tbl).join(units_tbl).join(units_tr_tbl).join(par_tbl).join(
                                      par_tr_tbl).filter(variable_tbl.columns['name'] == variable_name).filter(
                                          units_tr_tbl.columns['lang_code'] == ENGLISH_LANG_CODE).filter(
                                              par_tr_tbl.columns['lang_code'] == ENGLISH_LANG_CODE).one()
        except NoResultFound:
            print('{} variable {}'.format(
                '(DataAccess::_get_metadata) No records found in MDDB for', variable_name))
            raise

        info['data']['description']['@title'] = dataset_tbl_info.collection_name
        info['data']['description']['@name'] = var_tbl_info.parameter_name
        info['data']['description']['@units'] = var_tbl_info.units_name

        # Each vertical level is processed separately because corresponding arrays can be stored in different files
        for level_name in levels_names:
            info['data']['levels'][level_name] = {}

            level_name_pattern = '%:{0}:%'.format(level_name) # Pattern for LIKE in the following SQL-request
            # Get some info about the data array and file names template
            try:
                data_tbl_info = \
                    session.query(data_tbl.columns['scale'],
                                  data_tbl.columns['offset'],
                                  file_tbl.columns['name'].label('file_name_template'),
                                  file_tbl.columns['timestart'],
                                  file_tbl.columns['timeend'],
                                  levels_variable_tbl.columns['name'].label('level_variable_name'),
                                  units_tr_tbl.columns['name'].label('units_name')).join(dataset_tbl).join(
                                      variable_tbl).join(levels_tbl).join(file_tbl).join(levels_variable_tbl).join(
                                          units_tbl).join(units_tr_tbl).filter(
                                              dataset_tbl.columns['id'] == dataset_tbl_info.id).filter(
                                                  variable_tbl.columns['name'] == variable_name).filter(
                                                      levels_tbl.columns['name'].like(level_name_pattern)).filter(
                                                          units_tr_tbl.columns['lang_code'] == ENGLISH_LANG_CODE).one()
            except NoResultFound:
                print('{} collection: {}, scenario: {}, resolution: {}, time step: {}, variable: {}, level: {}'.format(
                    '(DataAccess::_get_metadata) No records found in MDDB for', dataset_name, scenario_name,
                    resolution_name, time_step_name, variable_name, level_name))
                raise

            info['data']['levels'][level_name]['@scale'] = data_tbl_info.scale
            info['data']['levels'][level_name]['@offset'] = data_tbl_info.offset
            file_name_template = '{0}{1}{2}{3}{4}'.format(dataset_tbl_info.rootpath, dataset_tbl_info.subpath0,
                                                          dataset_tbl_info.subpath1, dataset_tbl_info.subpath2,
                                                          data_tbl_info.file_name_template)
            info['data']['levels'][level_name]['@file_name_template'] = file_name_template
            info['data']['levels'][level_name]['@time_start'] = data_tbl_info.timestart
            info['data']['levels'][level_name]['@time_end'] = data_tbl_info.timeend
            info['data']['levels'][level_name]['@level_variable_name'] = data_tbl_info.level_variable_name

        return info

    def get(self, uid, segments=None, levels=None):
        """Reads data and metadata from an input data source (dataset, parameter, array).

        Arguments:
            uid -- processing module input's UID (as in a task file)
            segments -- list of time segments (read all if omitted)
            levels - list of vertical level (read all if omitted)
        """
        options = {}
        options['segments'] = segments
        options['levels'] = levels
        result = self._data_objects[uid].read(options)
        return result

    def input_uids(self):
        """Returns a list of UIDs of processing module inputs (as in a task file)"""

        return self._input_uids

    def get_segments(self, uid):
        """Returns time segments list

        Arguments:
            uid -- UID of a processing module's input (as in a task file)
        """
        if self._input_uids is not None:
            try:
                input_idx = self._input_uids.index(uid)
            except ValueError:
                print('(DataAccess::get_segments) No such input UID: ' + uid)
                raise
            segments = self._inputs[input_idx]['data']['time']['segment']
        else:
            segments = None
        return listify(segments)

    def get_levels(self, uid):
        """Returns vertical levels list

        Arguments:
            uid -- UID of a processing module's input  (as in a task file)
        """
        if self._input_uids is not None:
            try:
                input_idx = self._input_uids.index(uid)
            except ValueError:
                print('(DataAccess::get_segments) No such input UID: ' + uid)
                raise
            if isinstance(self._inputs[input_idx]['data']['levels']['@values'], set):
                levels = list(self._inputs[input_idx]['data']['levels']['@values'])
            else:
                levels = [level_name.strip() for level_name in self._inputs[input_idx]['data']['levels']['@values'].split(';')]
        else:
            levels = None
        return levels

    def put(self, uid, values, level=None, segment=None, times=None, longitudes=None, latitudes=None, fill_value=None,
            description=None, meta=None):
        """Writes data and metadata to an output data storage (array).

        Arguments:
            uid -- UID of a processing module's output (as in a task file)
            values -- processing result's values as a masked array/array/list
            level -- vertical level name segment -- time segment description (as in input time segments taken from a task file)
            times -- time grid as a list of datatime values
            longitudes -- longitude grid (1-D or 2-D) as an array/list
            latitudes -- latitude grid (1-D or 2-D) as an array/list
            fill_value -- fill value
            description -- dictionary describing data:
                ['title'] -- general title of the data (e.g., Average)
                ['name'] --  name of the data (e.g., Temperature)
                ['units'] -- units of the data (e.g., K)
            meta -- additional metadata passed from data readers to data writers through data processors
        """
        options = {}
        options['level'] = level
        options['segment'] = segment
        options['times'] = times
        options['longitudes'] = longitudes
        options['latitudes'] = latitudes
        if fill_value is not None:
            values.fill_value = fill_value
        options['description'] = description
        options['meta'] = meta
        self._data_objects[uid].write(values, options)

    def output_uids(self):
        """Returns a list of UIDs of processing module outputs (as in a task file)"""

        return self._output_uids

    def is_stations(self, uid):
        """Returns True if the dataset linked to the uid contains stations data.

        Arguments:
            uid -- UID of an input dataset (or datafile) to be tested.

        Returns:
            Boolean True if the dataset contains stations data, boolean False otherwise.
        """

        is_stations = False
        data_type = self._data_types.get(uid)
        if data_type is not None:
            if data_type.upper() == 'DB':  # We assume that stations are stored only in a PostGIS database.
                is_stations = True
        else:
            print('(DataAccess::is_stations) Warning: UID {} is not defined in the task file. False was returned.'.format(uid))

        return is_stations
