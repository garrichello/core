"""This is a collection of Celery tasks of the Computing and Visualizing Core backend subsystem.

It is used as a part of Celery task manager system to start the Core.
It creates an instance of the MainApp class and starts the Core with a given task..
"""
from __future__ import absolute_import, unicode_literals

import base64
from configparser import ConfigParser
import os
import io

from zipfile import ZipFile
import logging
import logging.handlers
import xmltodict
from celery import Celery
from celery.signals import after_setup_logger
from celery.app.log import TaskFormatter
#from celery.contrib import rdb

import core
from .task_generator import task_generator

# Read main configuration file.
core_config = ConfigParser()
core_config.read(os.path.join(str(os.path.dirname(__file__)), 'core_config.ini'))

app = Celery('core')  # Instantiate Celery application (it runs tasks).
app.config_from_object('celeryconfig')  # Celery config is in celeryconfig.py file.

logger = logging.getLogger()

@after_setup_logger.connect
def setup_loggers(my_logger, *args, **kwargs):
    file_log_format = '[%(asctime)s] - %(task_id)s - %(levelname)-8s (%(module)s::%(funcName)s) %(message)s'
    formatter = TaskFormatter(file_log_format, use_color=False)

    error_log_file = os.path.join(core_config['RPC']['log_dir'], 'errors.log')
    print('ERROR LOG: ', error_log_file)
    err_file_handler = logging.handlers.RotatingFileHandler(
        error_log_file, maxBytes=1048576, backupCount=5, encoding='utf8', delay=1)
    err_file_handler.setFormatter(formatter)
    err_file_handler.setLevel(logging.ERROR)
    my_logger.addHandler(err_file_handler)

    if core_config['RPC']['enable_core_log'] == 'yes':
        core_log_file = os.path.join(core_config['RPC']['log_dir'], 'core.log')
        print('CORE LOG: ', core_log_file)
        core_file_handler = logging.handlers.RotatingFileHandler(
            core_log_file, maxBytes=10485760, backupCount=5, encoding='utf8', delay=0)
        core_file_handler.setFormatter(formatter)
        core_file_handler.setLevel(logging.INFO)
        my_logger.addHandler(core_file_handler)

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
    task = xmltodict.parse(task_xml)
    result_zip = application.run_task(task, self.request.id)

    logger.info('Task %s is finished.', self.request.id)

    return base64.b64encode(result_zip).decode('utf-8')

@app.task(bind=True)
def run_json_task(self, json_task):
    """Basic Celery application task for starting the Core with a JSON byte-stream task.

    It creates an instance of the MainApp class and starts the Core.
    Everything inside this function is controlled by Celery.
    """

    logger.info('%s v.%s', core.__prog__, core.__version__)

    # Instantiate the Core!
    application = core.MainApp()

    # Generate XML tasks from the JSON task.
    xml_tasks = task_generator(json_task, self.request.id, core_config['METADB'])

    # Run the task processing by the Core!
    # Result is a zip-file as bytes.
    if len(xml_tasks) == 1:  # Simple task
        result_zip = application.run_task(xml_tasks[0], self.request.id)
    else:  # Complex task (with nested tasks)
        # We store intermediate results in a temporary directory.
        self.logger.info('Complex task was submitted')
        original_cwd_dir = os.getcwd()  # Store current directory.
        global_tmp_dir = core_config['RPC']['tmp_dir']
        nested_task_dir = os.path.join(global_tmp_dir, str(self.request.id)+'_intermediate')
        main_task_dir = os.path.join(global_tmp_dir, str(self.request.id))
        os.makedirs(nested_task_dir, exist_ok=True)
        os.chdir(nested_task_dir)  # Change to the intermediate task directory!
        self.logger.info('Run nested tasks first...')
        for xml_task in xml_tasks:
            if 'wait' in xml_task:  # Nested tasks are separated by the 'wait flag'.
                break
            result_zip = application.run_task(xml_task, self.request.id)
            mem_zip = io.BytesIO(result_zip)  # Zip-in-memeory buffer.
            with ZipFile(mem_zip, 'r') as zip_file:
                zip_file.extractall()
        self.logger.info('Done!')
        self.logger.info('Run the main task...')
        os.chdir(original_cwd_dir)  # Return to the original directory.
        os.rename(nested_task_dir, main_task_dir)  # Rename intermediate directory so main task could find intermediate results.
        result_zip = application.run_task(xml_tasks[-1], self.request.id)  # Run the main task.
        self.logger.info('Done!')

    logger.info('Task %s is finished.', self.request.id)

    return base64.b64encode(result_zip).decode('utf-8')

if __name__ == '__main__':
    app.start()
