import os
import json
import pprint
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from configparser import ConfigParser
from copy import copy

ENGLISH_LANG_CODE = 409
TIME_PERIOD_TYPES = {'PERIOD_GIVEN', 'PERIOD_DAY', 'PERIOD_MONTH', 'PERIOD_SEASON', 'PERIOD_YEAR'}

def make_time_segments(time_period, session, meta):
    ''' Creates a list of time segments as XML structure based on JSON structure.

    Arguments:
        time_period -- dictionary describing a time period
        session -- opened session to MDDB
        meta -- dictionary with MDDB tables

    Returns:
        time_segments -- list of time segments as XML structure
    '''

    time_period_type_tbl = meta.tables['time_period_type']
    qry = session.query(time_period_type_tbl.columns['const_name'])
    qry = qry.filter(time_period_type_tbl.columns['id'] == time_period['@type_id'])
    time_period_type = qry.one()[0]

    time_segments = []

    return time_segments

def make_levels(level_ids, session, meta):
    ''' Creates a string of level labels separated by a semicolon from level ids in MDDB.

    Arguments:
        level_ids -- list of level ids in MDDB
        session -- opened session to MDDB
        meta -- dictionary with MDDB tables

    Returns:
        level_labels -- string of level labels separated by a semicolon
    '''

    level_tbl = meta.tables['level']
    qry = session.query(level_tbl.columns['label'])
    level_names = [qry.filter(level_tbl.columns['id'] == level_id).one()[0] 
                   for level_id in level_ids]

    levels_string = ';'.join(level_names)

    return levels_string

def get_data_info(proc_argument, session, meta):
    ''' Extracts basic data info from MDDB

    Arguments:
        proc_argument -- JSON-based dictionary describing data
        session -- opened session to MDDB
        meta -- dictionary with MDDB tables

    Returns:
        data -- XML-based dictionary describing data
    '''
    # Single SQL to get everything:

    # SELECT collection_i18n.name AS collection_name,
    #        parameter_i18n.name AS parameter_name,
    #        units_i18n.name AS units_name,
    #        collection.label AS collection_label,
    #        resolution.name AS resolution_name,
    #        time_step.label AS time_step_label,
    #        scenario.name AS scenario_name,
    #        variable.name AS variable_name
    #   FROM data
    #   JOIN dataset ON dataset_collection_id = dataset.collection_id
    #    AND dataset_resolution_id = dataset.resolution_id
    #    AND dataset_scenario_id = dataset.scenario_id
    #   JOIN specific_parameter ON specific_parameter_parameter_id = specific_parameter.parameter_id
    #    AND specific_parameter_levels_group_id = specific_parameter.levels_group_id
    #    AND specific_parameter_time_step_id = specific_parameter.time_step_id
    #   JOIN units ON units_id = units.id
    #   JOIN variable ON variable_id = variable.id
    #   JOIN collection ON dataset.collection_id = collection.id
    #   JOIN resolution ON dataset.resolution_id = resolution.id
    #   JOIN scenario ON dataset.scenario_id = scenario.id
    #   JOIN parameter ON specific_parameter.parameter_id = parameter.id
    #   JOIN levels_group ON specific_parameter.levels_group_id = levels_group.id
    #   JOIN levels_group_has_level on levels_group.id = levels_group_has_level.levels_group_id
    #   JOIN level on levels_group_has_level.level_id = level.id
    #   JOIN time_step ON specific_parameter.time_step_id = time_step.id
    #   JOIN collection_i18n ON collection_i18n.collection_id = collection.id
    #   JOIN parameter_i18n ON parameter_i18n.parameter_id = parameter.id
    #   JOIN units_i18n ON units_i18n.units_id = units.id
    #  WHERE collection_i18n.language_code = 409
    #    AND parameter_i18n.language_code = 409
    #	 AND units_i18n.language_code = 409
    #    AND collection.id = 13
    #    AND parameter.id = 1
    #    AND resolution.id = 2
    #    AND scenario.id = 1
    #    AND time_step.id = 1

    # Tables in a metadata database
    collection_tbl = meta.tables['collection']
    collection_i18n_tbl = meta.tables['collection_i18n']
    scenario_tbl = meta.tables['scenario']
    resolution_tbl = meta.tables['resolution']
    time_step_tbl = meta.tables['time_step']
    dataset_tbl = meta.tables['dataset']
    data_tbl = meta.tables['data']
    variable_tbl = meta.tables['variable']
#    level_tbl = meta.tables['level']
#    levels_group_tbl = meta.tables['levels_group']
#    levels_group_has_level_tbl = meta.tables['levels_group_has_level']
    units_tbl = meta.tables['units']
    units_i18n_tbl = meta.tables['units_i18n']
    parameter_tbl = meta.tables['parameter']
    parameter_i18n_tbl = meta.tables['parameter_i18n']
    specific_parameter_tbl = meta.tables['specific_parameter']

    # Get info from MDDB.
    # Prepare a common query.
    qry = session.query(collection_i18n_tbl.columns['name'].label('collection_name'),
                        parameter_i18n_tbl.columns['name'].label('parameter_name'),
                        units_i18n_tbl.columns['name'].label('units_name'),
                        collection_tbl.columns['label'].label('collection_label'),
                        resolution_tbl.columns['name'].label('resolution_name'),
                        time_step_tbl.columns['label'].label('time_step_label'),
                        scenario_tbl.columns['name'].label('scenario_name'),
                        variable_tbl.columns['name'].label('variable_name')
                        )
    qry = qry.select_from(data_tbl)
    qry = qry.join(dataset_tbl)
    qry = qry.join(specific_parameter_tbl)
    qry = qry.join(units_tbl)
    qry = qry.join(variable_tbl)
    qry = qry.join(collection_tbl)
    qry = qry.join(resolution_tbl)
    qry = qry.join(scenario_tbl)
    qry = qry.join(parameter_tbl)
#    qry = qry.join(levels_group_tbl)
#    qry = qry.join(levels_group_has_level_tbl)
#    qry = qry.join(level_tbl)
    qry = qry.join(time_step_tbl)
    qry = qry.join(collection_i18n_tbl)
    qry = qry.join(parameter_i18n_tbl)
    qry = qry.join(units_i18n_tbl)
    qry = qry.filter(collection_i18n_tbl.columns['language_code'] == ENGLISH_LANG_CODE)
    qry = qry.filter(parameter_i18n_tbl.columns['language_code'] == ENGLISH_LANG_CODE)
    qry = qry.filter(units_i18n_tbl.columns['language_code'] == ENGLISH_LANG_CODE)
    qry = qry.filter(collection_tbl.columns['id'] == proc_argument['data']['@collection_id'])
    qry = qry.filter(parameter_tbl.columns['id'] == proc_argument['data']['@parameter_id'])
    qry = qry.filter(resolution_tbl.columns['id'] == proc_argument['data']['@resolution_id'])
    qry = qry.filter(scenario_tbl.columns['id'] == proc_argument['data']['@scenario_id'])
    qry = qry.filter(time_step_tbl.columns['id'] == proc_argument['data']['@timeStep_id'])
    qry = qry.distinct()

    return qry.one()

def make_data(proc_argument, session, meta):
    ''' Creates XML data description based on JSON argument.

    Arguments:
        proc_argument -- JSON-based dictionary describing data
        session -- opened session to MDDB
        tables -- dictionary with MDDB tables

    Returns:
        data -- XML-based dictionary describing data
    '''

    res = get_data_info(proc_argument, session, meta)

    data = {}
    data['@uid'] = 'Data_{}'.format(proc_argument['@position'])
    data['@type'] = 'dataset'

    data['description'] = {}
    data['description']['@title'] = res.collection_name  # collection_i18n.name
    data['description']['@name'] = res.parameter_name  # parameter_i18n.name
    data['description']['@units'] = res.units_name  # units_i18n.name

    data['dataset'] = {}
    data['dataset']['@name'] = res.collection_label  # collection.label
    data['dataset']['@resolution'] = res.resolution_name  # resolution.name
    data['dataset']['@time_step'] = res.time_step_label  # time_step.label
    data['dataset']['@scenario'] = res.scenario_name  # scenario.name

    data['variable'] = {}
    data['variable']['@name'] = res.variable_name  # variable.name
    data['variable']['@tempk2c'] = 'no'  # convert temperature from K to C, default value: 'no'

    data['region'] = {}
    data['region']['@units'] = 'degrees'  # units of points coordinates describing ROI, default value: 'degrees'
    data['region']['@kind'] = proc_argument['data']['region']['@kind']
    data['region']['point'] = copy(proc_argument['data']['region']['point'])

    data['levels'] = {}
    levels = proc_argument['data']['level']
    if not isinstance(levels, list):  # Convert single valued dictionary to a list.
        levels = [levels]
    level_ids = [level['@id'] for level in levels]  # Extract level ids.
    data['levels']['@values'] = make_levels(level_ids, session, meta)  # level.label

    data['time'] = {}
    data['time']['@template'] = 'YYYYMMDDHH'  # Default time template.
    data['time']['segment'] = make_time_segments(proc_argument['data']['timePeriod'], session, meta)


    return data

def make_processing(proc):
    return 0

def task_generator(json_task, task_id, metadb_info, nested=False):
    ''' Creates a list of dictionaries reflecting an XML task files according to a JSON task description.
    Nested processing is supported. Nested tasks will be placed in the list before the outer task.
    Dictionary with a key 'wait' separates nested tasks and the outer one to support parallel execution.

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
        tasks -- list of dictionaries reflecting XML task file structures. These tasks will be executed consecutively/parallelly.
    '''

    # Connect to metadata DB
    db_url = '{0}://{1}@{2}/{3}'.format(metadb_info['engine'], metadb_info['user'], metadb_info['host'], metadb_info['name'])
    engine = create_engine(db_url)
    meta = MetaData()
    meta.reflect(bind=engine)
    session_class = sessionmaker(bind=engine)
    session = session_class()

    # All tasks list
    tasks = []

    # Create a basic task
    task = {}
    task['@uid'] = task_id
    task['metadb'] = {'@host': metadb_info['host'],
                      '@name': metadb_info['name'],
                      '@user': metadb_info['user'],
                      '@password': metadb_info['password']}
    task['data'] = []

    for arg in json_task['processing']['argument']:
        if 'data' in arg.keys():
            task['data'].append(make_data(arg, session, meta))
        elif 'processing' in arg.keys():
            nested_tasks = task_generator(arg, task_id, metadb_info, True)
            tasks.extend(nested_tasks)

    if len(tasks) > 0:
        tasks.append({'wait': True})  # Append wait signal after nested tasks.
    tasks.append(task)

    return tasks

if __name__ == "__main__":
    core_config = ConfigParser()
    core_config.read(os.path.join(str(os.path.dirname(__file__)),'core_config.ini'))
    JSON_FILE_NAME = '..\\very_simple_task.json'
    with open(JSON_FILE_NAME, 'r') as json_file:
        j_task = json.load(json_file)
    task = task_generator(j_task, 1, core_config['METADB'])
    pprint.pprint(task)
