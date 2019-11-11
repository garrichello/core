"""This is a collection of Celery tasks of the Computing and Visualizing Core backend subsystem.

It is used as a part of Celery task manager system to start the Core.
It creates an instance of the MainApp class and starts the Core with a given task..
"""
from __future__ import absolute_import, unicode_literals

import base64
from configparser import ConfigParser
import os

import logging
import logging.handlers
from celery import Celery
from celery.signals import after_setup_logger
from celery.app.log import TaskFormatter

import core

# Read main configuration file.
core_config = ConfigParser()
core_config.read(os.path.join(str(os.path.dirname(__file__)),'core_config.ini'))

app = Celery('core')  # Instantiate Celery application (it runs tasks).
app.config_from_object('celeryconfig')  # Celery config is in celeryconfig.py file.

logger = logging.getLogger()

@after_setup_logger.connect
def setup_loggers(logger, *args, **kwargs):
    file_log_format = '[%(asctime)s] - %(task_id)s - %(levelname)-8s (%(module)s::%(funcName)s) %(message)s'
    formatter = TaskFormatter(file_log_format, use_color=False)

    error_log_file = os.path.join(core_config['RPC']['log_dir'], 'errors.log')
    print('ERROR LOG IS WRITTEN HERE: ', error_log_file)
    file_handler = logging.handlers.RotatingFileHandler(error_log_file, maxBytes=1048576, backupCount=5, encoding='utf8', delay=1)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.ERROR)
    logger.addHandler(file_handler)

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
