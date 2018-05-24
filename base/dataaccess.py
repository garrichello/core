"""Class-helper for accessing data.
Provides access to data through unified API for processing modules.
"""

class DataAccess():
    def __init__(self, inputs, outputs, metadb_info):
        """Initialize class's attributes
        
        Arguments:
            inputs -- list of dictionaries describing input arguments of the processing module
            outputs -- list of dictionaries describing output arguments of the processing module
            metadb_info -- dictionary describing metadata database (location and user credentials)
        """
        self._inputs = inputs
        self._outputs = outputs
        self._metadb = metadb_info
        