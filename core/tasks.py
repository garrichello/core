"""This is a collection of Celery tasks of the Computing and Visualizing Core backend subsystem.

It is used as a part of Celery task manager system to start the Core.
It creates an instance of the MainApp class and starts the Core with a given task..
"""
from __future__ import absolute_import, unicode_literals

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

    application = core.MainApp()  # Instance of the Core
    result = application.run_task(task_xml, self.request.id)  # Run the Core!

    with open('output.zip', 'wb') as out_file:
        out_file.write(result)
    out_file.close()

    return 'TASK COMPLETED!'

if __name__ == '__main__':
    app.start()
