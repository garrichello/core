"""Provides classes:
    DataAccess
"""

from copy import copy, deepcopy
import logging
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.orm.exc import NoResultFound
from .common import load_module, make_module_name, listify

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

        ::get_data_info(uid):
        Returns full data description
            uid -- UID of a processing module's input (as in a task file)

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

        self.logger = logging.getLogger()
        self._input_uids = []  # UIDs of input data sources.
        self._output_uids = []  # UID of output data destinations.
        self._data_objects = {}  # Instanses of data access classes.
        self._data_types = {}   # Types of datasets.

        # Process input arguments: None - no inputs; get metadata for each data source (if any)
        # and instantiate corresponding classes.
        self.logger.info('Prepare inputs...')
        self._inputs = listify(inputs)
        if self._inputs is None:
            self._input_uids = None
        else:
            for input_num, input_ in enumerate(self._inputs):
                uid = input_['@uid']
                if not uid in self._input_uids:
                    self._input_uids.append(uid)
                else:
                    self.logger.error('Error! Duplicate input data UID: %s. Aborting.', uid)
                    raise ValueError
                # Get additional info about an input from the metadata database
                input_info = self._get_metadata(metadb_info, input_)
                #  Data access class name is: 'Data' + <data type name> (e.g., DataNetcdf)
                data_class_name = 'Data' + input_info['@data_type'].capitalize()
                self.logger.info('Input data module #%s: %s', input_num+1, data_class_name)
                module_name = make_module_name(data_class_name)
                data_class = load_module(module_name, data_class_name, package_name=self.__module__)
                if input_['data'].get('@object') is None:
                    input_['data']['@object'] = data_class(input_info)  # Try to instantiate data reading class
                self._data_objects[uid] = input_['data']['@object']
                self._data_types[uid] = input_info['@data_type']

        self.logger.info('Done!')

        # Process ouput argumetns: None - no outputs; get metadata for each data destination (if any)
        # and instantiate corresponding classes.
        self.logger.info('Prepare outputs...')
        self._outputs = listify(outputs)
        if outputs is None:
            self._output_uids = None
        else:
            for output_num, output_ in enumerate(self._outputs):
                uid = output_['@uid']
                if not uid in self._output_uids:
                    self._output_uids.append(uid)
                else:
                    self.logger.error('Error! Duplicate output data UID: %s. Aborting.', uid)
                    raise ValueError
                # Get additional info about an output from the metadata database
                output_info = self._get_metadata(metadb_info, output_)
                #  Data access class name is: "Data" + <File type name> (e.g., DataNetcdf)
                data_class_name = 'Data' + output_info['@data_type'].capitalize()
                self.logger.info('Output data module #%s: %s', output_num+1, data_class_name)
                module_name = make_module_name(data_class_name)
                data_class = load_module(module_name, data_class_name, package_name=self.__module__)
                if output_['data'].get('@object') is None:
                    output_['data']['@object'] = data_class(output_info)  # Try to instantiate data writing class
                self._data_objects[uid] = output_['data']['@object']
                self._data_types[uid] = output_info['@data_type']

        self.logger.info('Done!')

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

            # Raw file additional info
            if argument['data']['@type'] == 'raw' and 'levels' in argument['data'].keys():
                levels_names = [level_name.strip() for level_name in argument['data']['levels']['@values'].split(';')]
                for level_name in levels_names:
                    info['data']['levels'][level_name] = {}
                    info['data']['levels'][level_name]['@scale'] = 1.0
                    info['data']['levels'][level_name]['@offset'] = 0.0
                    info['data']['levels'][level_name]['@file_name_template'] = argument['data']['file']['@name']
                    info['data']['levels'][level_name]['@level_variable_name'] = 'level'

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
        collection_i18n_tbl = meta.tables['collection_i18n']
        scenario_tbl = meta.tables['scenario']
        resolution_tbl = meta.tables['resolution']
        time_step_tbl = meta.tables['time_step']
        dataset_tbl = meta.tables['dataset']
        file_type_tbl = meta.tables['file_type']
        data_tbl = meta.tables['data']
        file_tbl = meta.tables['file']
        variable_tbl = meta.tables['variable']
        level_tbl = meta.tables['level']
        levels_group_tbl = meta.tables['levels_group']
        levels_group_has_level_tbl = meta.tables['levels_group_has_level']
        levels_variable_tbl = aliased(variable_tbl)
        root_dir_tbl = meta.tables['root_dir']
        units_tbl = meta.tables['units']
        units_i18n_tbl = meta.tables['units_i18n']
        parameter_tbl = meta.tables['parameter']
        parameter_i18n_tbl = meta.tables['parameter_i18n']
        specific_parameter_tbl = meta.tables['specific_parameter']

        # Values for SQL-conditions
        dataset_name = argument['data']['dataset']['@name']
        scenario_name = argument['data']['dataset']['@scenario']
        resolution_name = argument['data']['dataset']['@resolution']
        time_step_name = argument['data']['dataset']['@time_step']
        variable_name = argument['data']['variable']['@name']
        levels_names = [level_name.strip() for level_name in argument['data']['levels']['@values'].split(';')]

        # Single SQL to get everything:
        # SELECT file_type.name AS file_type_name,
        #        collection_i18n.name AS collection_name,
        #        parameter_i18n.name AS parameter_name,
        #        units_i18n.name AS units_name,
        #        parameter.accumulation_mode AS acc_mode,
        #        data.scale AS scale,
        #        data.offset AS offset,
        #        root_dir.name AS rootpath,
        #        scenario.subpath0,
        #        resolution.subpath1,
        #        time_step.subpath2,
        #        file.name_pattern AS file_name_template,
        #        level.label as level_name,
        #        levels_variable.name AS level_variable_name
        #   FROM data
        #   JOIN dataset ON dataset_collection_id = dataset.collection_id
        #    AND dataset_resolution_id = dataset.resolution_id
        #    AND dataset_scenario_id = dataset.scenario_id
        #   JOIN specific_parameter ON specific_parameter_parameter_id = specific_parameter.parameter_id
        #    AND specific_parameter_levels_group_id = specific_parameter.levels_group_id
        #    AND specific_parameter_time_step_id = specific_parameter.time_step_id
        #   JOIN units ON units_id = units.id
        #   JOIN variable ON variable_id = variable.id
        #   JOIN file ON file_id = file.id
        #   JOIN levels_variable ON levels_variable_id = levels_variable.id
        #   JOIN root_dir ON root_dir_id = root_dir.id
        #   JOIN collection ON dataset.collection_id = collection.id
        #   JOIN resolution ON dataset.resolution_id = resolution.id
        #   JOIN scenario ON dataset.scenario_id = scenario.id
        #   JOIN parameter ON specific_parameter.parameter_id = parameter.id
        #   JOIN levels_group ON specific_parameter.levels_group_id = levels_group.id
        #   JOIN levels_group_has_level on levels_group.id = levels_group_has_level.levels_group_id
        #   JOIN level on levels_group_has_level.level_id = level.id
        #   JOIN time_step ON specific_parameter.time_step_id = time_step.id
        #   JOIN file_type ON file.file_type_id = file_type.id
        #   JOIN time_span ON file.time_span_id = time_span.id
        #   JOIN collection_i18n ON collection_i18n.collection_id = collection.id
        #   JOIN parameter_i18n ON parameter_i18n.parameter_id = parameter.id
        #   JOIN units_i18n ON units_i18n.units_id = units.id
        #  WHERE collection_i18n.language_code = ENGLISH_LANG_CODE
        #    AND parameter_i18n.language_code = ENGLISH_LANG_CODE
        #    AND units_i18n.language_code = ENGLISH_LANG_CODE
        #    AND collection.label = dataset_name
        #    AND scenario.name = scenario_name
        #    AND resolution.name = resolution_name
        #    AND time_step.label = time_step_name
        #    AND variable.name = variable_name
        #    AND level.label = level_name_pattern

        # Get info from MDDB.
        # Prepare a common query.
        qry = session.query(file_type_tbl.columns['name'].label('file_type_name'),
                            collection_i18n_tbl.columns['name'].label('collection_name'),
                            parameter_i18n_tbl.columns['name'].label('parameter_name'),
                            units_i18n_tbl.columns['name'].label('units_name'),
                            parameter_tbl.columns['accumulation_mode'].label('acc_mode'),
                            data_tbl.columns['scale'],
                            data_tbl.columns['offset'],
                            root_dir_tbl.columns['name'].label('root_dir'),
                            scenario_tbl.columns['subpath0'],
                            resolution_tbl.columns['subpath1'],
                            time_step_tbl.columns['subpath2'],
                            file_tbl.columns['name_pattern'].label('file_name_template'),
                            levels_variable_tbl.columns['name'].label('level_variable_name')
                            )
        qry = qry.select_from(data_tbl)
        qry = qry.join(dataset_tbl)
        qry = qry.join(specific_parameter_tbl)
        qry = qry.join(units_tbl)
        qry = qry.join(variable_tbl, variable_tbl.c.id == data_tbl.c.variable_id)
        qry = qry.join(file_tbl)
        qry = qry.outerjoin(levels_variable_tbl, levels_variable_tbl.c.id == data_tbl.c.levels_variable_id)
        qry = qry.join(root_dir_tbl)
        qry = qry.join(collection_tbl)
        qry = qry.join(resolution_tbl)
        qry = qry.join(scenario_tbl)
        qry = qry.join(parameter_tbl)
        qry = qry.join(levels_group_tbl)
        qry = qry.join(levels_group_has_level_tbl)
        qry = qry.join(level_tbl)
        qry = qry.join(time_step_tbl)
        qry = qry.join(file_type_tbl)
        qry = qry.join(collection_i18n_tbl)
        qry = qry.join(parameter_i18n_tbl)
        qry = qry.join(units_i18n_tbl)
        qry = qry.filter(collection_i18n_tbl.columns['language_code'] == ENGLISH_LANG_CODE)
        qry = qry.filter(parameter_i18n_tbl.columns['language_code'] == ENGLISH_LANG_CODE)
        qry = qry.filter(units_i18n_tbl.columns['language_code'] == ENGLISH_LANG_CODE)
        qry = qry.filter(collection_tbl.columns['label'] == dataset_name)
        qry = qry.filter(scenario_tbl.columns['name'] == scenario_name)
        qry = qry.filter(resolution_tbl.columns['name'] == resolution_name)
        qry = qry.filter(time_step_tbl.columns['label'] == time_step_name)
        qry = qry.filter(variable_tbl.columns['name'] == variable_name)

        # Each vertical level is processed separately because corresponding arrays can be stored in different files
        for level_name in levels_names:
            info['data']['levels'][level_name] = {}

            # Get some info
            try:
                data_info = qry.filter(level_tbl.columns['label'] == level_name).one()

            except NoResultFound:
                self.logger.error('No records found in MDDB for collection: %s, scenario: %s, resolution: %s, time step: %s, variable: %s, level: %s',
                                  dataset_name, scenario_name, resolution_name, time_step_name, variable_name, level_name)
                raise

            info['data']['levels'][level_name]['@scale'] = data_info.scale
            info['data']['levels'][level_name]['@offset'] = data_info.offset
            file_name_template = '{0}{1}{2}{3}{4}'.format(data_info.root_dir, data_info.subpath0,
                                                          data_info.subpath1, data_info.subpath2,
                                                          data_info.file_name_template)
            info['data']['levels'][level_name]['@file_name_template'] = file_name_template
            info['data']['levels'][level_name]['@level_variable_name'] = data_info.level_variable_name

        info['@data_type'] = data_info.file_type_name
        info['data']['description']['@title'] = data_info.collection_name
        info['data']['description']['@name'] = data_info.parameter_name
        info['data']['description']['@units'] = data_info.units_name
        info['data']['description']['@acc_mode'] = data_info.acc_mode

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
                self.logger.error('No such input UID: %s', uid)
                raise
            if 'time' in self._inputs[input_idx]['data'].keys():  # Parameters do not have time, so need to check.
                segments = self._inputs[input_idx]['data']['time']['segment']
            else:
                segments = None
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
                self.logger.error('No such input UID: %s', uid)
                raise
            if 'levels' in self._inputs[input_idx]['data'].keys():  # Parameters do not have levels, so need to check.
                if isinstance(self._inputs[input_idx]['data']['levels']['@values'], set):
                    levels = list(self._inputs[input_idx]['data']['levels']['@values'])
                else:
                    levels = [level_name.strip() for level_name in self._inputs[input_idx]['data']['levels']['@values'].split(';')]
            else:
                levels = None
        else:
            levels = None
        return levels

    def get_data_info(self, uid):
        """Returns full data info

        Arguments:
            uid -- UID of a processing module's input (as in a task file)
        """
        data = None
        if self._input_uids is not None:
            if uid in self._input_uids:
                idx = self._input_uids.index(uid)
                data = self._inputs[idx]['data']
        if self._output_uids is not None:
            if uid in self._output_uids:
                if data is not None:  # uid is both in inputs and outputs!
                    self.logger.error('Requested UID %s is presented in both inputs and outputs. Check the task!', uid)
                    raise ValueError
                idx = self._output_uids.index(uid)
                data = self._outputs[idx]['data']
        if data is None:
            self.logger.error('Requested UID %s is not found! Aborting', uid)
            raise ValueError
        return data

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
        options['segment'] = copy(segment)
        options['times'] = copy(times)
        options['longitudes'] = copy(longitudes)
        options['latitudes'] = copy(latitudes)
        if fill_value is not None:
            values.fill_value = fill_value
        options['description'] = deepcopy(description)
        options['meta'] = deepcopy(meta)
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
            self.logger.warning('Warning: UID %s is not defined in the task file. False was returned.', uid)

        return is_stations
