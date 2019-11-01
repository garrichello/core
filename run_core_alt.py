"""This is a main program of the Computing and Visualizing Core backend subsystem.

It is used to start the Core.
It handles command line arguments and creates an instance of the MainApp class.
"""
import sys
import time
import traceback

import core

if __name__ == '__main__':
    """Main function.

    It creates an instance of the MainApp class and runs the application."""

    task_xml_lines = []

    for line in sys.stdin:
        task_xml_lines.append(line)
    task_xml = ''.join(task_xml_lines)

    print(core.__prog__ + ' v.' + core.__version__)

    start_time = time.time()
    app = core.MainApp()

    try:
        app.run_task(task_xml)
        print('SUCCESS!')
    except:
        print("ERROR!")
        traceback.print_exc()

    end_time = time.time()

    exec_time = end_time - start_time
    print('Total execution time: {0:8.2f} s'.format(exec_time))
