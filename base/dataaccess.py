"""Class-helper for accessing data.
Provides access to data through unified API for processing modules.
"""

from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker

from base.common import load_module

class DataAccess():
    def __init__(self, inputs, outputs, metadb_info):
        """Initialize class's attributes.
        
        Arguments:
            inputs -- list of dictionaries describing input arguments of the processing module
            outputs -- list of dictionaries describing output arguments of the processing module
            metadb_info -- dictionary describing metadata database (location and user credentials)
        """

        self._input_uids = []
        self._data_classes = {}

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
                data_class_name = "data" + input_info["filetype"].capitalize() #  Data access class name is: "Data" + <File type name> (e.g., DataNetcdf)
                self._data_classes[uid]["class"] = load_module("mod", data_class_name) # Try to instantiate data processing class
                self._data_classes[uid]["info"] = input_info # Add metadata to the class
                
        self._outputs = outputs
        if outputs is None:
            self._output_uids = None
        else:
            if not isinstance(outputs, list):
                self._outputs = [outputs]
            self._output_uids = [output_['@uid'] for output_ in self._outputs]

        self._metadb = metadb_info
        
    def _get_metadata(self, metadb_info, argument):
        """Loads metadata from metadata database for an argument (input or output of the processing module)

        Arguments:
            metadb_info -- dictionary containing information about metadata database.
            argument -- dictionary containing description of the processing module's argument

        Returns: dictionary containing metadata for the 'argument;
        """
        db_url = "mysql://{0}@{1}/{2}".format(metadb_info['@user'], metadb_info['@host'], metadb_info['@name']) # metadata database URL
        engine = create_engine(db_url)
        meta = MetaData(bind = engine, reflect = True)
        
        collection = meta.tables["collection"]
        scenario = meta.tables["scenario"] 
        resolution = meta.tables["res"]
        time_step = meta.tables["tstep"]

        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Get collection id
        dataset_name = argument["data"]["dataset"]["@name"]
        collection_id = session.query(collection.columns["id"]).filter(
                collection.columns["name"] == dataset_name).all()[0]

        # Get scenario id and subpath
        scenario_name = argument["data"]["dataset"]["@scenario"]
        scenario_id, subpath0 = session.query(scenario.columns["id"], scenario.columns["subpath0"]).filter(
                scenario.columns["name"] == scenario_name).all()[0]

        # Get horizontal resolution id and subpath
        resolution_name = argument["data"]["dataset"]["@resolution"]
        resolution_id, subpath1 = session.query(resolution.columns["id"], resolution.columns["subpath1"]).filter(
                resolution.columns["name"] == resolution_name).all()[0]

        # Get time step id and subpath
        time_step_name = argument["data"]["dataset"]["@time_step"]
        time_step_id, subpath2 = session.query(time_step.columns["id"], time_step.columns["subpath2"]).filter(
                time_step.columns["name"] == time_step_name).all()[0]

        # 
        levels_num = len(argument["data"]["levels"]["@values"].split(';')) # Number of vertical levels (separated by semicolon)

        print("(DataAccess::_get_metadata) Finished!")
    
    def get(self, uid, segments=None, levels=None):
        """Reads data and metadata from an input data source (dataset, parameter, array).

        Arguments:
            uid -- processing module input's UID (as in a task file)
            segments -- list of time segments (read all if omitted)
            levels - list of vertical level (read all if omitted)
        """
        pass

    def get_uids(self):
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

