"""This is a main program of the Computing and Visualizing Core backend subsystem.

It is used to start the Core.
It handles command line arguments and creates an instance of the MainApp class.
"""
import argparse
import time

from .base import MainApp

__prog__ = 'Core'
__version__ = '1.0a'
__author__ = 'Igor Okladnikov'


def main(args):
    """Main function.

    It creates an instance of the MainApp class and runs the application."""
    print(__prog__ + ' v.' + __version__)

    start_time = time.time()
    app = MainApp(args)

#    try:
    app.run()
    print('SUCCESS!')
#    except:
#        print("ERROR!")
    end_time = time.time()

    exec_time = end_time - start_time
    print('Total execution time: {0:8.2f} s'.format(exec_time))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Computing and Visualizing Core')
    parser.add_argument('task_file_name', help='name of an XML task file')
    arguments = parser.parse_args()

    main(arguments)
