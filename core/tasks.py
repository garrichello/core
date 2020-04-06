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
from celery import Celery, group
from celery.signals import after_setup_logger
from celery.app.log import TaskFormatter
import xmltodict

import core
from .task_generator import task_generator

# Read main configuration file.
core_config = ConfigParser()
core_config.read(os.path.join(str(os.path.dirname(__file__)), 'core_config.ini'))

app = Celery('core')  # Instantiate Celery application (it runs tasks).
app.config_from_object('celeryconfig')  # Celery config is in celeryconfig.py file.

logger = logging.getLogger()

def write_xml_task(task, task_id, pos=0):
    global_tmp_dir = core_config['RPC']['tmp_dir']

    # Write prepared XML-task to a temporary directory.
    xml_file_name = os.path.join(global_tmp_dir, '{}_task_{}.xml'.format(task_id, pos))
    with open(xml_file_name, 'w') as xml_file:
        xmltodict.unparse(task, xml_file, pretty=True)

@after_setup_logger.connect
def setup_loggers(*args, **kwargs):
    """ Setup custom logging """
    file_log_format = '[%(asctime)s] - %(task_id)s - %(levelname)-8s (%(module)s::%(funcName)s) %(message)s'
    formatter = TaskFormatter(file_log_format, use_color=False)

    error_log_file = os.path.join(core_config['RPC']['log_dir'], 'errors.log')
#    print('ERROR LOG: ', error_log_file)
    err_file_handler = logging.handlers.RotatingFileHandler(
        error_log_file, maxBytes=1048576, backupCount=5, encoding='utf8', delay=1)
    err_file_handler.setFormatter(formatter)
    err_file_handler.setLevel(logging.ERROR)
    logger.addHandler(err_file_handler)

    if core_config['RPC']['enable_core_log'] == 'yes':
        core_log_file = os.path.join(core_config['RPC']['log_dir'], 'core.log')
#        print('CORE LOG: ', core_log_file)
        core_file_handler = logging.handlers.RotatingFileHandler(
            core_log_file, maxBytes=10485760, backupCount=5, encoding='utf8', delay=0)
        core_file_handler.setFormatter(formatter)
        core_file_handler.setLevel(logging.INFO)
        logger.addHandler(core_file_handler)
    trace_logger = logging.getLogger('celery.app.trace')
    trace_logger.propagate = True

@app.task(bind=True)
def worker(self, result_files_list, task):
    """Creates an instance of the MainApp class and starts the Core.

    Arguments:
        result_files_list -- list of byte-array objects representing zip-archives
                             with results of nested tasks.
        task -- dictionary containing description of a task as in an XML file

    Returns:
        result_zip -- base64-encoded byte-array object representing a zip-archive with processing results
    """

    logger.info('%s v.%s', core.__prog__, core.__version__)

    # Instantiate the Core!
    application = core.MainApp()

    if result_files_list is not None:
        global_tmp_dir = core_config['RPC']['tmp_dir']
        task_dir = os.path.join(global_tmp_dir, str(self.request.id))
        os.makedirs(task_dir, exist_ok=True)
        for result_zip_enc in result_files_list:
            result_zip = base64.b64decode(result_zip_enc)
            mem_zip = io.BytesIO(result_zip)  # Zip-in-memory buffer.
            with ZipFile(mem_zip, 'r') as zip_file:
                zip_file.extractall(path=task_dir)

    # Run the task processing by the Core!
    # Result is a zip-file as bytes.
    result_zip = application.run_task(task, self.request.id)

    logger.info('Task %s is finished.', self.request.id)

    return base64.b64encode(result_zip).decode('utf-8')

@app.task(bind=True)
def starter(self, json_task):
    """Starts workers. Controls a processing chain.

    Arguments:
        json_task -- JSON message containing a processing request.

    Returns:
        result_zip -- base64-encoded byte-array representing zip-archive with processing results.
    """

    task_timeout = 600  # No task can run for more than 10 min.

    # Generate XML tasks from the JSON task.
    xml_tasks = task_generator(json_task, self.request.id, core_config['METADB'])
    #for i, xml_task in enumerate(xml_tasks):
    #    write_xml_task(xml_task, self.request.id, i)

    # Run the task processing by the Core! Using another task. A task in a task. :)
    # Result is a zip-file as bytes base64-encoded.
    if len(xml_tasks) == 1:  # Simple task
        result = worker.s(None, xml_tasks[0]).apply_async(routing_key='worker.abak.scert.ru')
    else:  # Complex task (with nested tasks)
        subtasks = []  # Collect nested tasks in a group.
        for xml_task in xml_tasks:
            if 'wait' in xml_task:  # Nested tasks are separated by the 'wait flag'.
                break
            subtasks.append(worker.s(None, xml_task).set(routing_key='worker.abak.scert.ru'))
        job = (group(subtasks) | worker.s(xml_tasks[-1]).set(routing_key='worker.abak.scert.ru'))
        result = job()

    logger.info('Task %s is finished.', self.request.id)

    result_zip = result.get(disable_sync_subtasks=False, timeout=task_timeout)

    return {'data': result_zip, 'task': json_task, 'xml': [xmltodict.unparse(xml_task, pretty=True) for xml_task in xml_tasks]}

if __name__ == '__main__':
    app.start()
