"""Contains classes:
    MainApp
"""
from copy import deepcopy
import logging
import os
from configparser import ConfigParser

import collections
import xmltodict

from .proc import Proc
from .common import listify

class MainApp:
    """Main application class. It does everything the application does."""

    def __init__(self):
        """Parses command line arguments, extracts a task file name."

        Arguments:
            args - argparse's Namespace with command line arguments of the application.
        """

        self.logger = logging.getLogger()
        self._task = {}
        self._data_uid_list = []
        self._destination_uid_list = []
        self._task_id = None

        self.config = ConfigParser()
        self.config.read('../core_config.ini')

    def run(self, args):
        """Run this function to run the Core."""

        self.logger.info('Let\'s do it!')

        task_file_name = args.task_file_name
        self._read_task(task_file_name)
        self._process()

        self.logger.info('Job is done. Exiting.')

    def run_task(self, task_string, task_id=None):
        """Reads the task from a string and creates all necessary structures."""

        self.logger.info('Let\'s do it!')
        self.logger.info('Read the task...')

        self._task = xmltodict.parse(task_string)
        self._task_id = task_id

        # Make them lists!
        self._task['task']['data'] = listify(self._task['task']['data'])
        self._task['task']['destination'] = listify(self._task['task']['destination'])
        self._task['task']['processing'] = listify(self._task['task']['processing'])

        self.logger.info('Done!')

        # Modify task id in task file.
        self._task['task']['@uid'] = str(task_id)
        # Change location and names of output files.
        for destination in self._task['task']['destination']:
            _, ext = os.path.splitext(destination['file']['@name'])
            pool_dir = self.config['RPC']['pool_dir']
            destination['file']['@name'] = os.path.join(pool_dir, str(task_id), str(task_id)+ext.lower())

        self._process()

        self.logger.info('Job is done. Exiting.')

    def _read_task(self, task_file_name):
        """Reads the task file and creates all necessary structures."""

        self.logger.info('Read the task file...')

        try:
            with open(task_file_name) as file_descriptor:
                self._task = xmltodict.parse(file_descriptor.read())
        except FileNotFoundError:
            self.logger.error('Task file not found: %s', task_file_name)
            raise
        except UnicodeDecodeError:
            with open(task_file_name, encoding='windows-1251') as file_descriptor:
                self._task = xmltodict.parse(file_descriptor.read())

        # Make them lists!
        self._task['task']['data'] = listify(self._task['task']['data'])
        self._task['task']['destination'] = listify(self._task['task']['destination'])
        self._task['task']['processing'] = listify(self._task['task']['processing'])

        self.logger.info('Done!')

    def _dict_append(self, source, destination):
        if isinstance(source, dict):
            for k, v in source.items():
                if k not in destination.keys():
                    destination[k] = deepcopy(v)
                else:
                    self._dict_append(source[k], destination[k])

    def _inherit_properties(self, task, parent_uid, child_uid):
        """Allows to inherit properties of a parent data element conserving existing properties of a child.

        Arguments:
            task -- current task dictionary
            parent_uid -- parent data UID
            child_uid -- child data UID

        Returns:
            'data' dictionary with properties of the parent data overridden with existing properties of a child.
        """
        child_idx = self._data_uid_list.index(child_uid)
        try:
            parent_idx = self._data_uid_list.index(parent_uid) # Search for a parent 'data' element.
        except ValueError:
            self.logger.error('Can\'t find parent data UID \'%s\' in child data \'%s\'', parent_uid, child_uid)
        child_data = task['data'][child_idx]
        parent_data = task['data'][parent_idx]
        self._dict_append(parent_data, child_data)
        if child_data.get('@product'):
            child_data['variable']['@name'] += '_' + child_data.get('@product')  # Suffix for the base variable name.

        return child_data

    def _prepare_proc_arguments(self, task, proc_uid, proc_args):
        """Adds a new 'data' element into the argument's dictionary
        containing a full description of the data/destination argument.

        Arguments:
            task -- task dictionary (used in generating an error message)
            proc_uid -- UID of the current processing (used in generating an error message)
            proc_args -- input or output arguments dictionary
        """

        # If there is only one input or output item, create a list with this item, so the following 'for' can work normally.
        if isinstance(proc_args, collections.OrderedDict):
            proc_args = [proc_args]
        for arg in proc_args:
            argument_uid = arg['@data'] # UID of the data/destination argument.
            if argument_uid in self._data_uid_list:
                data_idx = self._data_uid_list.index(argument_uid) # Search for a 'data' element.
                parent_uid = task['data'][data_idx].get('@parent')  # UID of the parent data (which properties it should inherit).
                if parent_uid:
                    arg['data'] = self._inherit_properties(task, parent_uid, argument_uid)
                else:
                    arg['data'] = task['data'][data_idx]  # Add a new dictionary item with a description.
                arg_description = arg['data'].get('description')  # Get arguments description.
                source_uid = None
                if arg_description:
                    source_uid = arg_description.get('@source')  # Check if the arguments description has 'source' attribute.
                if source_uid:
                    source_idx = self._data_uid_list.index(source_uid)
                    arg['data']['description']['@name'] = task['data'][source_idx]['description']['@name']  # Get name from source UID.
                    arg['data']['description']['@units'] = task['data'][source_idx]['description']['@units']  # Get units from source UID.
            elif argument_uid in self._destination_uid_list:
                data_idx = self._destination_uid_list.index(argument_uid) # Search for a 'destination' element.
                arg['data'] = task['destination'][data_idx] # Add a new dictionary item with a description.
            else:
                self.logger.error('Can\'t find data or destination UID: \'%s\' in processing \'%s\' input \'%s\'',
                                  argument_uid, proc_uid, arg['@uid'])
                raise ValueError

    def _process(self):
        """Runs modules in the order specified in a task file."""

        self.logger.info('Start the processing.')

        for task_name in self._task:
            task = self._task[task_name]
            metadb_info = task['metadb'] # Location of the metadata database and user credentials to access it.
            self._data_uid_list = [data['@uid'] for data in task['data']] # List of all data UIDs.
            self._destination_uid_list = [destination['@uid'] for destination in task['destination']] # List of all destination UIDs.

            # Run processings one by one as specified in a task file.
            for proc in task['processing']:
                proc_class_name = proc['@class'] # The name of a processing class.

                # For each processing prepare a set of input and output arguments.
                # Each argument contains description of the corresponding piece of data or visualization options.
                proc_input = proc.get('input')
                if proc_input is not None:
                    self._prepare_proc_arguments(task, proc['@uid'], proc_input)

                proc_output = proc.get('output')
                if proc_output is not None:
                    self._prepare_proc_arguments(task, proc['@uid'], proc_output)

                # Initiate the computing processor (class that runs and controls processing modules).
                processor = Proc(proc_class_name, proc_input, proc_output, metadb_info)

                # Run the processor which in turn should run the processing module.
                processor.run()

        self.logger.info('Processing is finished.')
