"""Containes classes:
    MainApp
"""

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

    def process(self):
        """Runs and controls modules"""
        print("(MainApp::process) Run the processing")

