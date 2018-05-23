"""Containes classes:
    MainApp
"""
import importlib
import collections

import xmltodict

class MainApp:
    """Main application class. It does everythiong the application does."""  

    def __init__(self, args=None):
        """Parses command line arguments, extracts a task file name"
        
        Arguments:
            args - argparse's Namespace with command line arguments of the application
        """
        self.task_file_name = args.task_file_name
    
    def run(self):
        """Run this function to run the Core."""
        print("(MainApp::run) Let's do it!")

        self.read_task()

        self.process()

    def read_task(self):
        """Reads the task file and creates all necessary structures"""
        print("(MainApp::read_task) Read the task file")
        
        with open(self.task_file_name) as fd:
            self.task = xmltodict.parse(fd.read())

    def prepare_proc_arguments(self, task, proc, proc_args):
        data_uid = [data['@uid'] for data in task['data']]
        destination_uid = [destination['@uid'] for destination in task['destination']]
        # if there is only one input or output item, create a list with this item, so the following 'for' can work normally
        if type(proc_args) is collections.OrderedDict:
            proc_args = [proc_args]
        for arg in proc_args:
            try: # try to find a 'data' element and replace data UID with it's description
                data_idx = data_uid.index(arg['@data'])
                arg['description'] = task['data'][data_idx]
            except ValueError: # if not found...
                try: # ...try to find a 'destination' element and replace data UID with it's description
                    data_idx = destination_uid.index(arg['@data'])
                    arg['description'] = task['destination'][data_idx]
                except ValueError: # if not found print error message and abort
                    print("(MainApp::process) Can't find data or destination UID: '" 
                            + arg['@data'] + "' in processing '" + proc['@uid'] 
                            + "' input '" + arg['@uid'] + "'")
                    raise

    def load_module(self, module_name, class_name):
        """Loads module by its name and returns class to instantiate

        Arguments:
            module_name -- name of the Python module (file name)
            class_name -- name of the class in this module
        """
        try:
            module_ = importlib.import_module(module_name)
            try:
                class_ = getattr(module_, class_name)
            except AttributeError:
                print("(MainApp::load_module) Class " + class_name + " does not exist")
        except ImportError:
            print("(MainApp::load_module) Module " + module_name + " does not exist")
        return class_ or None

    def process(self):
        """Runs and controls modules"""
        print("(MainApp::process) Run the processing")

        for task_name in self.task:
            task = self.task[task_name]
            metadb_info = task['metadb'] # location of the metadata database and user credentials to access it

            # we run processings one by one as specified in the task file
            for proc in task['processing']:
                proc_class_name = proc['@class'] # name of the processing module

                # for each processing we prepare a set of input and output arguments
                # each argument contains description of the corresponding piece of data or visualization options
                proc_input = proc.get('input')
                if proc_input is not None:
                    self.prepare_proc_arguments(task, proc, proc_input)

                proc_output = proc.get('output')
                if proc_output is not None:
                    self.prepare_proc_arguments(task, proc, proc_output)
                           
                # let's try to create an instance of the processing class
                proc_class = self.load_module("mod", proc_class_name)
                processor = proc_class(proc_input, proc_output, metadb_info)
                processor.run()
