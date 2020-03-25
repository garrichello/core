import os
import json
import pprint
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from configparser import ConfigParser

def make_data(proc_argument):
    return 0

def task_generator(json_task, task_id, metadb_info):
    ''' Creates a task dictionary reflecting an XML task file according to a JSON task description.

    Arguments:
        json_task -- dictionary containing JSON task description
        task_id -- task id
        metadb_info -- config parser dictionary containing essential info on metadata db:
            'engine': db engine name ('mysql')
            'host': host name or ip
            'name': database name
            'user': user name
            'password': user password

    Returns:
        task -- dictionary reflecting XML task file structure
    '''

    # Connect to metadata DB
    db_url = '{0}://{1}@{2}/{3}'.format(metadb_info['engine'], metadb_info['user'], metadb_info['host'], metadb_info['name'])
    engine = create_engine(db_url)
    meta = MetaData()
    meta.reflect(bind=engine)
    session_class = sessionmaker(bind=engine)
    session = session_class()

    # Create the basic task
    task = {}
    task['@uid'] = task_id
    task['metadb'] = {'@host': metadb_info['host'],
                      '@name': metadb_info['name'],
                      '@user': metadb_info['user'],
                      '@password': metadb_info['password']}
    task['data'] = []
    for arg in json_task['processing']['argument']:
        task['data'].append(make_data(arg))

    task['destination'] = []
    task['processing'] = []

    return task


if __name__ == "__main__":
    core_config = ConfigParser()
    core_config.read(os.path.join(str(os.path.dirname(__file__)),'core_config.ini'))
    JSON_FILE_NAME = 'C:\\Users\\Garry\\OneDrive\\Workspace\\Python\\very_simple_task.json'
    with open(JSON_FILE_NAME, 'r') as json_file:
        j_task = json.load(json_file)
    task = task_generator(j_task, 1, core_config['METADB'])
    pprint.pprint(task)
