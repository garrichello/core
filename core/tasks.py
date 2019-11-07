"""This is a collection of Celery tasks of the Computing and Visualizing Core backend subsystem.

It is used as a part of Celery task manager system to start the Core.
It creates an instance of the MainApp class and starts the Core with a given task..
"""
from __future__ import absolute_import, unicode_literals
import sys
import time
import traceback

from kombu import Exchange, Queue

import core

from celery import Celery

app = Celery('tasks')  # Instantiate Celery application (it runs tasks).
app.config_from_object('celeryconfig')  # Celery config is in celeryconfig.py file.

@app.task(bind=True)
def run_plain_xml(self, task_xml):
    """Basic Celery application task for starting the Core with a plain XML byte-stream task.

    It creates an instance of the MainApp class and starts the Core.
    Everything inside this function is controlled by Celery.
    """

    print(core.__prog__ + ' v.' + core.__version__)
    print('Running task id: {}'.format(self.request.id))
    start_time = time.time()

    application = core.MainApp()  # Instance of the Core
    application.run_task(task_xml, self.request.id)  # Run the Core!

    end_time = time.time()
    exec_time = end_time - start_time
    print('Total execution time: {0:8.2f} s'.format(exec_time))

    return 'TASK COMPLETED!'

if __name__ == '__main__':
        app.start()

