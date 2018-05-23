"""This is a main program of the Computing and Visualizing Core backend susbsystem.

It is used to start the Core. 
It handles command line arguments and creates an instance of the MainApp class.
"""


__prog__ = "Core"
__version__ = "0.1"
__author__ = "Igor Okladnikov"

import argparse

import base

def main(args):
    """Main function.

    It creates an instance of the MainApp class and runs the application."""
    print(__prog__ + " v. " + __version__)

    app = base.MainApp(args)

    try:
        app.run()
        print("SUCCESS!")
    except:
        print("ERROR!")
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = "Computing and Visualizing Core")
    parser.add_argument("task_file_name", help = "name of an XML task file")
    args = parser.parse_args()
    
    main(args)

