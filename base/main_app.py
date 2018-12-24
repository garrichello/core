"""Contains classes:
    MainApp
"""
import collections
import xmltodict

from base.proc import Proc
from base.common import print

class MainApp:
    """Main application class. It does everything the application does."""

    def __init__(self, args=None):
        """Parses command line arguments, extracts a task file name."

        Arguments:
            args - argparse's Namespace with command line arguments of the application.
        """

        self._task_file_name = args.task_file_name
        self._task = {}
        self._data_uid_list = []
        self._destination_uid_list = []

    def run(self):
        """Run this function to run the Core."""

        print('(MainApp::run) Let\'s do it!')

        self._read_task()
        self._process()

        print('(MainApp::run) Job is done. Exiting.')

    def _read_task(self):
        """Reads the task file and creates all necessary structures."""

        print("(MainApp::read_task) Read the task file...")

        try:
            with open(self._task_file_name) as file_descriptor:
                self._task = xmltodict.parse(file_descriptor.read())
        except FileNotFoundError:
            print('(MainApp::_read_task) Task file not found: ' + self._task_file_name)
            raise
        except UnicodeDecodeError:
            with open(self._task_file_name, encoding='windows-1251') as file_descriptor:
                self._task = xmltodict.parse(file_descriptor.read())

        print("(MainApp::read_task) Done!")

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
                arg['data'] = task['data'][data_idx] # Add a new dictionary item with a description.
                source_uid = arg['data']['description'].get('@source')  # Check if the argument has 'source' attribute
                if source_uid is not None:
                    source_idx = self._data_uid_list.index(source_uid)
                    arg['data']['description']['@name'] = task['data'][source_idx]['description']['@name']  # Get name from source UID
                    arg['data']['description']['@units'] = task['data'][source_idx]['description']['@units']  # Get units from source UID
            elif argument_uid in self._destination_uid_list:
                data_idx = self._destination_uid_list.index(argument_uid) # Search for a 'destination' element.
                arg['data'] = task['destination'][data_idx] # Add a new dictionary item with a description.
            else:
                print('(MainApp::process) Can\'t find data or destination UID: \''
                      + argument_uid + '\' in processing \'' + proc_uid
                      + '\' input \'' + arg['@uid'] + '\'')
                raise ValueError

    def _process(self):
        """Runs modules in the order specified in a task file."""

        print('(MainApp::process) Start the processing.')

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

        print('(MainApp::process) Processing is finished.')
