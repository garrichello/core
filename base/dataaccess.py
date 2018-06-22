"""Provides classes:
    DataAccess
"""

from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from base.common import load_module, listify, print

class DataAccess():
    """Class-helper for accessing data.
    Provides access to data through unified API for processing modules.
    """    
    def __init__(self, inputs, outputs, metadb_info):
        """Initializes class's attributes. Reads metadata database. 
        Instantiate classes-readers and classes-writers for input and output arguments of a processing module correspondingly.
        
        Arguments:
            inputs -- list of dictionaries describing input arguments of a processing module
            outputs -- list of dictionaries describing output arguments of a processing module
            metadb_info -- dictionary describing metadata database (location and user credentials)
        """

        self._input_uids = [] # UIDs of input data sources.
        self._output_uids = [] # UID of output data destinations.
        self._data_readers = {} # Instanses of classes-readers of data.
        self._data_writers = {}  # Instances of classes-writers of data.

        # Process input arguments: None - no inputs; get metadata for each data source (if any) and instantiate corresponding classes.
        print("(DataAccess::__init__) Prepare inputs...")
        self._inputs = listify(inputs)
        if self._inputs is None:
            self._input_uids = None
        else:
            for input_ in self._inputs:
                uid = input_['@uid']
                self._input_uids.append(uid)
                input_info = self._get_metadata(metadb_info, input_) # Get additional info about an input from the metadata database
                data_class_name = "Data" + input_info["@data_type"].capitalize() #  Data access class name is: "Data" + <File type name> (e.g., DataNetcdf)
                input_class = load_module("mod", data_class_name)
                self._data_readers[uid] = input_class(input_info)  # Try to instantiate data reading class
                
        # Process ouput argumetns: None - no outputs; get metadata for each data destination (if any) and instantiate corresponding classes.
        print("(DataAccess::__init__) Prepare outputs...")
        self._outputs = listify(outputs)
        if outputs is None:
            self._output_uids = None
        else:
            for output_ in self._outputs:
                uid = output_['@uid']
                self._output_uids.append(uid)
                output_info = self._get_metadata(metadb_info, output_) # Get additional info about an output from the metadata database
                data_class_name = "Data" + output_info["@data_type"].capitalize() #  Data access class name is: "Data" + <File type name> (e.g., DataNetcdf)
                output_class = load_module("mod", data_class_name)
                self._data_writers[uid] = output_class(output_info)  # Try to instantiate data writing class

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
        if argument["data"]["@type"] != "dataset":
            info["@data_type"] = argument["data"]["@type"]
            info["data"] = argument["data"]
            return info

        # If it is a dataset there is much to do
        info["data"] = argument["data"] # All the information about the dataset is passed to the data-accessing modules
        db_url = "mysql://{0}@{1}/{2}".format(metadb_info['@user'], metadb_info['@host'], metadb_info['@name']) # metadata database URL
        engine = create_engine(db_url)
        meta = MetaData(bind = engine, reflect = True)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Tables in a metadata database
        collection_tbl = meta.tables["collection"]
        scenario_tbl = meta.tables["scenario"] 
        resolution_tbl = meta.tables["res"]
        time_step_tbl = meta.tables["tstep"]
        dataset_tbl = meta.tables["ds"]
        file_type_tbl = meta.tables["filetype"]
        data_tbl = meta.tables["data"]
        file_tbl = meta.tables["file"]
        variable_tbl = meta.tables["var"]
        levels_tbl = meta.tables["lvs"]
        levels_variable_tbl = meta.tables["lvs_var"]
        dataset_root_tbl = meta.tables["dsroot"]
        time_span_tbl = meta.tables["timespan"]

        # Values for SQL-conditions
        dataset_name = argument["data"]["dataset"]["@name"]
        scenario_name = argument["data"]["dataset"]["@scenario"]
        resolution_name = argument["data"]["dataset"]["@resolution"]
        time_step_name = argument["data"]["dataset"]["@time_step"]
        variable_name = argument["data"]["variable"]["@name"]
        levels_names = [level_name.strip() for level_name in argument["data"]["levels"]["@values"].split(';')]
 
        # Get some info about the dataset.
        try:
            dataset_tbl_info = session.query(dataset_tbl.columns["id"],
                    file_type_tbl.columns["name"].label("file_type_name"), 
                    scenario_tbl.columns["subpath0"], 
                    resolution_tbl.columns["subpath1"], 
                    time_step_tbl.columns["subpath2"],
                    time_span_tbl.columns["name"].label("file_time_span"),
                    dataset_root_tbl.columns["rootpath"]).join(
                        collection_tbl).join(scenario_tbl).join(resolution_tbl).join(
                        time_step_tbl).join(file_type_tbl).join(dataset_root_tbl).join(time_span_tbl).filter(
                            collection_tbl.columns["name"] == dataset_name).filter(
                            scenario_tbl.columns["name"] == scenario_name).filter(
                            resolution_tbl.columns["name"] == resolution_name).filter(
                            time_step_tbl.columns["name"] == time_step_name).one()
        except NoResultFound:
            print("{} collection: {}, scenario: {}, resolution: {}, time step: {}".format(
                "(DataAccess::_get_metadata) No records found in MDDB for", dataset_name, scenario_name,
                resolution_name, time_step_name))
            raise

        info["@data_type"] = dataset_tbl_info.file_type_name
        info["@file_time_span"] = dataset_tbl_info.file_time_span

        # Each vertical level is processed separately because corresponding arrays can be stored in different files
        info["levels"] = {}
        for level_name in levels_names:
            info["levels"][level_name] = {}

            level_name_pattern = '%:{0}:%'.format(level_name) # Pattern for LIKE in the following SQL-request
            # Get some info about the data array and file names template
            try:
                data_tbl_info = session.query(data_tbl.columns["scale"], 
                        data_tbl.columns["offset"], 
                        file_tbl.columns["name"].label("file_name_template"), 
                        file_tbl.columns["timestart"],
                        file_tbl.columns["timeend"],
                        levels_variable_tbl.columns["name"].label("level_variable_name")).join(dataset_tbl).join(
                            variable_tbl).join(levels_tbl).join(file_tbl).join(levels_variable_tbl).filter(
                                dataset_tbl.columns["id"] == dataset_tbl_info.id).filter(
                                variable_tbl.columns["name"] == variable_name).filter(
                                levels_tbl.columns["name"].like(level_name_pattern)).one()
            except NoResultFound:
                print("{} collection: {}, scenario: {}, resolution: {}, time step: {}, variable: {}, level: {}".format(
                    "(DataAccess::_get_metadata) No records found in MDDB for", dataset_name, scenario_name,
                    resolution_name, time_step_name, variable_name, level_name))
                raise

            info["levels"][level_name]["@scale"] = data_tbl_info.scale
            info["levels"][level_name]["@offset"] = data_tbl_info.offset
            file_name_template = "{0}{1}{2}{3}{4}".format(dataset_tbl_info.rootpath, dataset_tbl_info.subpath0, 
                    dataset_tbl_info.subpath1, dataset_tbl_info.subpath2, data_tbl_info.file_name_template)
            info["levels"][level_name]["@file_name_template"] = file_name_template
            info["levels"][level_name]["@time_start"] = data_tbl_info.timestart
            info["levels"][level_name]["@time_end"] = data_tbl_info.timeend
            info["levels"][level_name]["@level_variable_name"] = data_tbl_info.level_variable_name

        return info
    
    def get(self, uid, segments=None, levels=None):
        """Reads data and metadata from an input data source (dataset, parameter, array).

        Arguments:
            uid -- processing module input's UID (as in a task file)
            segments -- list of time segments (read all if omitted)
            levels - list of vertical level (read all if omitted)
        """
        result = self._data_readers[uid].read(segments, levels)
        return result

    def input_uids(self):
        """Returns a list of UIDs of processing module inputs (as in a task file)"""
        
        return self._input_uids

    def get_segments(self, uid):
        """Returns time segments list
        
        Arguments:
            uid -- UID of a processing module's input (as in a task file)
        """
        if type(self._input_uids) is not None:
            try:
                input_idx = self._input_uids.index(uid)
            except ValueError:
                print("(DataAccess::get_segments) No such input UID: " + uid)
                raise
            segments = self._inputs[input_idx]['data']['time']['segment']
        else:
            segments = None
        return segments

    def get_levels(self, uid):
        """Returns vertical levels list
        
        Arguments:
            uid -- UID of a processing module's input  (as in a task file)
        """
        if type(self._input_uids) is not None:
            try:
                input_idx = self._input_uids.index(uid)
            except ValueError:
                print("(DataAccess::get_segments) No such input UID: " + uid)
                raise
            levels = [level_name.strip() for level_name in self._inputs[input_idx]["data"]["levels"]["@values"].split(';')]
        else:
            levels = None
        return levels

    def put(self, uid, values, level = None, segment = None, times = None, longitudes = None, latitudes = None):
        """Writes data and metadata to an output data storage (array).

        Arguments:
            uid -- UID of a processing module's output (as in a task file)
            values -- processing result's values as a masked array/array/list
            level -- vertical level name segment -- time segment description (as in input time segments taken from a task file)
            times -- time grid as a list of datatime values
            longitudes -- longitude grid (1-D or 2-D) as an array/list
            latitudes -- latitude grid (1-D or 2-D) as an array/list
        """

        self._data_writers[uid].write(values, level, segment, times, longitudes, latitudes)
        pass

    def output_uids(self):
        """Returns a list of UIDs of processing module outputs (as in a task file)"""
        
        return self._output_uids

