"""Contains classes:
    MainApp
"""
from copy import deepcopy
import logging
import os
import shutil
from configparser import ConfigParser
from zipfile import ZipFile, ZIP_DEFLATED
import io

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

        # Read main configuration file.
        self.config = ConfigParser()
        self.config.read(os.path.join(str(os.path.dirname(__file__)),
                                      '../core_config.ini'))

    def run(self, args):
        """Run this function to run the Core."""

        self.logger.info('Let\'s do it!')

        task_file_name = args.task_file_name
        self._read_task(task_file_name)
        self._process()

        self.logger.info('Job is done. Exiting.')

    def run_task(self, task, task_id=None):
        """Gets the task and creates necessary structures. Runs the task. Returns results as a zip-archive."""

        # Zips to a memory buffer processing results in the current work directory.
        def zip_results():
            self.logger.info('Compress results...')
            mem_zip = io.BytesIO()  # Zip-in-memeory buffer.
            with ZipFile(mem_zip, 'w', ZIP_DEFLATED) as zip_file:
                # Read result files and add them to the zip-file.
                for file_name in os.listdir():
                    with open(file_name, 'rb') as result_file:
                        file_data = result_file.read()
                    zip_file.writestr(file_name, file_data)

            self.logger.info('Done!')

            buffer = mem_zip.getvalue()  # Get zip-file as plain bytes.
            self.logger.info('Zip-file length is %s bytes', len(buffer))

            return buffer

        self.logger.info('Let\'s do it!')
        self.logger.info('Read the task...')

        self._task = task
        self._task_id = task_id

        # Make them lists!
        self._task['task']['data'] = listify(self._task['task']['data'])
        self._task['task']['destination'] = listify(self._task['task']['destination'])
        self._task['task']['processing'] = listify(self._task['task']['processing'])

        self.logger.info('Done!')

        # Modify task id in task file.
        self._task['task']['@uid'] = str(task_id)

        # Make task dir
        original_cwd_dir = os.getcwd()  # Store current directory.
        tmp_dir = self.config['BASIC']['tmp_dir']
        task_dir = os.path.join(tmp_dir, str(task_id))
        os.makedirs(task_dir, exist_ok=True)
        os.chdir(task_dir)  # Move to the task directory!

        # Change location of output files.
#        for destination in self._task['task']['destination']:
#            file_name = os.path.basename(destination['file']['@name'])
#            destination['file']['@name'] = file_name
#            if destination['@type'] == 'image':
#                sld_name = os.path.basename(destination['graphics']['legend']['file']['@name'])
#                destination['graphics']['legend']['file']['@name'] = sld_name

        # Make a copy of the original task
        original_task = deepcopy(task)
        try:
            # Run task processing.
            self._process()

            # Compress results in memory.
            zip_buffer = zip_results()

        except:
            log_dir = self.config['BASIC']['log_dir']
            err_task_file = os.path.join(original_cwd_dir, log_dir,
                                         'error_task_'+str(task_id)+'.xml')
            with open(err_task_file, 'w') as out_file:
                xmltodict.unparse(original_task, out_file, pretty=True)
            raise
        finally:
            # Delete result files
            self.logger.info('Clean temporary files...')
            os.chdir(original_cwd_dir)  # Return to the original CWD.
            shutil.rmtree(task_dir)  # Delete task directory with all its contents
            self.logger.info('Done!')

        self.logger.info('Job is done. Exiting.')

        return zip_buffer

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

#    def _dict_append(self, source, destination):
#        if isinstance(source, dict):
#            for k, v in source.items():
#                if k not in destination.keys():
#                    destination[k] = deepcopy(v)
#                else:
#                    self._dict_append(source[k], destination[k])

#    def _inherit_properties(self, task, parent_uid, child_uid):
#        """Allows to inherit properties of a parent data element conserving existing properties of a child.
#
#        Arguments:
#            task -- current task dictionary
#            parent_uid -- parent data UID
#            child_uid -- child data UID
#
#        Returns:
#            'data' dictionary with properties of the parent data overridden with existing properties of a child.
#        """
#        child_idx = self._data_uid_list.index(child_uid)
#        try:
#            parent_idx = self._data_uid_list.index(parent_uid) # Search for a parent 'data' element.
#        except ValueError:
#            self.logger.error('Can\'t find parent data UID \'%s\' in child data \'%s\'', parent_uid, child_uid)
#        child_data = task['data'][child_idx]
#        parent_data = task['data'][parent_idx]
#        self._dict_append(parent_data, child_data)
#        if child_data.get('@product'):
#            child_data['variable']['@name'] += '_' + child_data.get('@product')  # Suffix for the base variable name.
#
#        return child_data

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
#                parent_uid = task['data'][data_idx].get('@parent')  # UID of the parent data (which properties it should inherit).
#                if parent_uid:
#                    arg['data'] = self._inherit_properties(task, parent_uid, argument_uid)
#                else:
                arg['data'] = task['data'][data_idx]  # Add a new dictionary item with a description.
#                arg_description = arg['data'].get('description')  # Get arguments description.
#                source_uid = None
#                if arg_description:
#                    source_uid = arg_description.get('@source')  # Check if the arguments description has 'source' attribute.
#                if source_uid:
#                    source_idx = self._data_uid_list.index(source_uid)
#                    arg['data']['description']['@name'] = task['data'][source_idx]['description']['@name']  # Get name from source UID.
#                    arg['data']['description']['@units'] = task['data'][source_idx]['description']['@units']  # Get units from source UID.
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
