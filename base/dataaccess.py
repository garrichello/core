"""Provides classes:
    DataAccess
"""

from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker

from base.common import load_module

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
        self._inputs = inputs
        if inputs is None:
            self._input_uids = None
        else:
            if not isinstance(inputs, list):
                self._inputs = [inputs]
            for input_ in self._inputs:
                uid = input_['@uid']
                self._input_uids.append(uid)
                input_info = self._get_metadata(metadb_info, input_) # Get additional info about an input from the metadata database
                data_class_name = "Data" + input_info["data_type"].capitalize() #  Data access class name is: "Data" + <File type name> (e.g., DataNetcdf)
                input_class = load_module("mod", data_class_name)
                self._data_readers[uid] = input_class(input_info)  # Try to instantiate data reading class
                
        # Process ouput argumetns: None - no outputs; get metadata for each data destination (if any) and instantiate corresponding classes.
        self._outputs = outputs
        if outputs is None:
            self._output_uids = None
        else:
            if not isinstance(outputs, list):
                self._outputs = [outputs]
            for output_ in self._outputs:
                uid = output_['@uid']
                self._output_uids.append(uid)
                output_info = self._get_metadata(metadb_info, output_) # Get additional info about an output from the metadata database
                data_class_name = "Data" + output_info["data_type"].capitalize() #  Data access class name is: "Data" + <File type name> (e.g., DataNetcdf)
                output_class = load_module("mod", data_class_name)
                self._data_writers[uid] = output_class(output_info)  # Try to instantiate data writing class

        self._metadb = metadb_info
        
    def _get_metadata(self, metadb_info, argument):
        """Loads metadata from metadata database for an argument (input or output of the processing module)

        Arguments:
            metadb_info -- dictionary containing information about metadata database.
            argument -- dictionary containing description of the processing module's argument

        Returns: dictionary containing metadata for the 'argument':
            ["data_type"] -- file type for a dataset (e.g., netcdf), data type otherwise (e.g.: parameter, array).

        """
        info = {} # Argument's information

        # We don't need to do much if it is not a dataset
        if argument["data"]["@type"] != "dataset":
            info["data_type"] = argument["data"]["@type"]
            info["data"] = argument["data"]
            return info

        # If it is a dataset there is much to do
        db_url = "mysql://{0}@{1}/{2}".format(metadb_info['@user'], metadb_info['@host'], metadb_info['@name']) # metadata database URL
        engine = create_engine(db_url)
        meta = MetaData(bind = engine, reflect = True)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Tables in a metadata database
        collection = meta.tables["collection"]
        scenario = meta.tables["scenario"] 
        resolution = meta.tables["res"]
        time_step = meta.tables["tstep"]
        dataset = meta.tables["ds"]
        file_type = meta.tables["filetype"]
        
        # Values for SQL-conditions
        dataset_name = argument["data"]["dataset"]["@name"]
        scenario_name = argument["data"]["dataset"]["@scenario"]
        resolution_name = argument["data"]["dataset"]["@resolution"]
        time_step_name = argument["data"]["dataset"]["@time_step"]

        # Get some info
        dataset_id, file_type_name = session.query(dataset.columns["id"], file_type.columns["name"]).join(
                collection).join(scenario).join(resolution).join(time_step).join(file_type).filter(
                    collection.columns["name"] == dataset_name).filter(
                        scenario.columns["name"] == scenario_name).filter(
                            resolution.columns["name"] == resolution_name).filter(
                                time_step.columns["name"] == time_step_name).one()

        info["data_type"] = file_type_name

        levels_num = len(argument["data"]["levels"]["@values"].split(';')) # Number of vertical levels (separated by semicolon)


        print("(DataAccess::_get_metadata) Finished!")
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
            uid -- processing module input's UID (as in a task file)
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
            uid -- processing module input's UID (as in a task file)
        """
        if type(self._input_uids) is not None:
            try:
                input_idx = self._input_uids.index(uid)
            except ValueError:
                print("(DataAccess::get_segments) No such input UID: " + uid)
                raise
            levels = self._inputs[input_idx]['data']['levels']['@values']
        else:
            levels = None
        return levels

    def put(self):
        """Writes data and metadata to an output data storage (array).

        Arguments:

        """
        pass

