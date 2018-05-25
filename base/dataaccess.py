"""Class-helper for accessing data.
Provides access to data through unified API for processing modules.
"""

class DataAccess():
    def __init__(self, inputs, outputs, metadb_info):
        """Initialize class's attributes.
        
        Arguments:
            inputs -- list of dictionaries describing input arguments of the processing module
            outputs -- list of dictionaries describing output arguments of the processing module
            metadb_info -- dictionary describing metadata database (location and user credentials)
        """
        self._inputs = inputs
        if inputs is None:
            self._input_uids = None
        else:
            if not isinstance(inputs, list):
                self._inputs = [inputs]
            self._input_uids = [input_['@uid'] for input_ in self._inputs]

        self._outputs = outputs
        if outputs is None:
            self._output_uids = None
        else:
            if not isinstance(outputs, list):
                self._outputs = [outputs]
            self._output_uids = [output_['@uid'] for output_ in self._outputs]

        self._metadb = metadb_info
        
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

