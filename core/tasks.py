"""This is a collection of Celery tasks of the Computing and Visualizing Core backend subsystem.

It is used as a part of Celery task manager system to start the Core.
It creates an instance of the MainApp class and starts the Core with a given task..
"""
from __future__ import absolute_import, unicode_literals

import base64

from celery import Celery
from celery.utils.log import get_task_logger

import core

app = Celery('tasks')  # Instantiate Celery application (it runs tasks).
app.config_from_object('celeryconfig')  # Celery config is in celeryconfig.py file.
logger = get_task_logger(__name__)

@app.task(bind=True)
def run_plain_xml(self, task_xml):
    """Basic Celery application task for starting the Core with a plain XML byte-stream task.

    It creates an instance of the MainApp class and starts the Core.
    Everything inside this function is controlled by Celery.
    """

    logger.info('%s v.%s', core.__prog__, core.__version__)

    # Instantiate the Core!
    application = core.MainApp()

    # Run the task processing by the Core!
    # Result is a zip-file as bytes.
    result_zip = application.run_task(task_xml, self.request.id)

    # Control write of result zip-file.
    with open('output.zip', 'wb') as out_file:
        out_file.write(result_zip)
    out_file.close()

    logger.info('Task %s is finished.', self.request.id)

    return base64.b64encode(result_zip).decode('utf-8')

if __name__ == '__main__':
    app.start()
