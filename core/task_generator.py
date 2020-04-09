""" Converts JSON-based task to XML-based dictionary.
Original task arrives as a dictionary based on a JSON message sent from outside.
Function task_generator generates a list of one or more dictionaries 
 describing data and processing conveyors as in an old XML tasks.
 Nested processing conveyors (complex tasks) are supported.
"""
import os
import json
from configparser import ConfigParser
from copy import copy, deepcopy
import datetime
import calendar
import logging
from collections import defaultdict
import xmltodict

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.sql.expression import and_
from sqlalchemy.orm.exc import NoResultFound

ENGLISH_LANG_CODE = 409
TIME_PERIOD_TYPES = {'PERIOD_GIVEN', 'PERIOD_DAY', 'PERIOD_MONTH', 'PERIOD_SEASON', 'PERIOD_YEAR'}
logger = logging.getLogger()

def make_time_segments(time_period, session, meta):
    ''' Creates a list of time segments as XML structure based on JSON structure.

    Arguments:
        time_period -- dictionary describing a time period:
            '@type_id' -- (int) time period type id in MDDB
            'dateStart' -- (dict) first date of the time period:
                '@day' -- day
                '@month' -- month
                '@year' -- year
            'dateEnd' -- (dict) last date of the time period
                '@day' -- day
                '@month' -- month
                '@year' -- year
        session -- opened session to MDDB
        meta -- MDDB metadata

    Returns:
        time_segments -- list of time segments as XML structure
    '''

    # Get time period type from MDDB.
    time_period_type_tbl = meta.tables['time_period_type']
    qry = session.query(time_period_type_tbl.columns['const_name'])
    qry = qry.filter(time_period_type_tbl.columns['id'] == time_period['@type_id'])
    try:
        time_period_type = qry.one()[0]
    except NoResultFound:
        logger.error('Can\'t find type_id %s in MDDB table "time_period_type"', time_period['@type_id'])
        raise

    # Extract the first and the last dates of the time period.
    year = int(time_period['dateStart']['@year']) if time_period['dateStart']['@year'] else 1
    month = int(time_period['dateStart']['@month']) if time_period['dateStart']['@month'] else 1
    day = int(time_period['dateStart']['@day']) if time_period['dateStart']['@day'] else 1
    start_date = datetime.datetime(year, month, day, hour=0)

    year = int(time_period['dateEnd']['@year']) if time_period['dateEnd']['@year'] else 1
    month = int(time_period['dateEnd']['@month']) if time_period['dateEnd']['@month'] else 12
    day = int(time_period['dateEnd']['@day']) if time_period['dateEnd']['@day'] else calendar.monthrange(year, month)[1]
    end_date = datetime.datetime(year, month, day, hour=23)

    # Calculate number of segments.
    if time_period_type == 'PERIOD_GIVEN':
        num_segments = 1
    else:
        num_segments = end_date.year - start_date.year + 1

    # Generate time segments.
    time_segments = []
    for seg_i in range(num_segments):
        time_segments.append({'@name': 'Seg{}'.format(seg_i+1),
                              '@beginning': datetime.datetime(start_date.year+seg_i,
                                                              start_date.month,
                                                              start_date.day,
                                                              start_date.hour).strftime('%Y%m%d%H'),
                              '@ending': datetime.datetime(start_date.year+seg_i,
                                                           end_date.month,
                                                           end_date.day,
                                                           end_date.hour).strftime('%Y%m%d%H')})

    return time_segments

def make_levels(level_ids, session, meta):
    ''' Creates a string of level labels separated by a semicolon from level ids in MDDB.

    Arguments:
        level_ids -- list of level ids in MDDB
        session -- opened session to MDDB
        meta -- MDDB metadata

    Returns:
        level_labels -- string of level labels separated by a semicolon
    '''

    level_tbl = meta.tables['level']
    qry = session.query(level_tbl.columns['label'])
    level_names = []
    for level_id in level_ids:
        try:
            level_names.append(qry.filter(level_tbl.columns['id'] == level_id).scalar())
        except NoResultFound:
            logger.error('Can\'t find level_id %s in MDDB table "level"', level_id)
            raise

    levels_string = ';'.join(level_names)

    return levels_string

def get_data_info(proc_argument, session, meta):
    ''' Extracts basic data info from MDDB

    Arguments:
        proc_argument -- JSON-based dictionary describing data
        session -- opened session to MDDB
        meta -- MDDB metadata

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

    # Map MDDB tables.
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

    try:
        result = qry.one()
    except NoResultFound:
        logger.error('No records found in MDDB for collection_id: %s, scenario_id: %s, resolution_id: %s, time step id: %s, parameter_id: %s',
                     proc_argument['data']['@collection_id'], proc_argument['data']['@scenario_id'], proc_argument['data']['@resolution_id'],
                     proc_argument['data']['@timeStep_id'], proc_argument['data']['@parameter_id'])
        raise

    return result

def make_data_arguments(proc_argument, session, meta):
    ''' Creates XML data description based on JSON argument.

    Arguments:
        proc_argument -- JSON-based dictionary describing data
        session -- opened session to MDDB
        meta -- MDDB metadata

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

def delete_vertex(vertex_key, processing_graph):
    ''' Deletes vertex from a processing graph.

    Arguments:
        vertex_key -- key of the vertex in the processing graph
        processing_graph -- dictionary containing adjacency lists for vertices.

    Returns:
        None
    '''

    vertex = processing_graph[vertex_key]  # Vertex to delete.

    # We know how to delete a vertex if it has only one input or only one output edge.
    if len(vertex['uplinks']) > 1 and len(vertex['downlinks']) > 1:
        raise ValueError("Vertex has several inputs and sevral outputs. And it should be deleted. Don't know how. Aborting...")

    # Move uplink and downlink connections of adjacent vertices past the vertex to be deleted.
    for uplink in vertex['uplinks']:
        data_label = uplink['data_label']
        link_to = next(link for link in uplink['vertex']['downlinks'] if link['vertex'] == vertex)
        uplink['vertex']['downlinks'].remove(link_to)
        uplink['vertex']['downlinks'].extend(vertex['downlinks'])
        for downlink in vertex['downlinks']:
            downlink['data_label'] = data_label
            link_to = next(link for link in downlink['vertex']['uplinks'] if link['vertex'] == vertex)
            downlink['vertex']['uplinks'].remove(link_to)
            downlink['vertex']['uplinks'].extend(vertex['uplinks'])

    # Delete the vertex from the graph.
    del processing_graph[vertex_key]

def make_data_array(data_uid, title='Stub title', name='Stub name', units='Stub units'):
    ''' Creates structure describing data array for XML-based task

    Arguments:
        data_uid -- uid of the array
        title -- title of the data
        name -- name of the data array
        units -- units of the data

    Returns:
        data_array -- dictionary containing XML-based description of a data array
    '''

    data_array = {'@uid': data_uid,
                  '@type': 'array',
                  'description': {'@title': title,
                                  '@name': name,
                                  '@units': units}}


    return data_array

def make_image(image_uid, graphics_type):
    ''' Creates structure describing image destination for XML-based task

    Arguments:
        image_uid -- uid of the image
        graphics_type -- type of the graphics file

    Returns:
        image_info -- dictionary containing XML-based description of an image destination
    '''

    if graphics_type.lower() == 'geotiff':
        file_ext = 'tiff'
    elif graphics_type.lower() == 'shape':
        file_ext = 'shp'
    else:
        file_ext = '.unknown'

    legend_file_name = 'output.sld'
    legend_file_type = 'xml'
    n_legend_colors = 10
    n_legend_labels = n_legend_colors + 1

    image_info = {'@uid': image_uid,
                  '@type': 'image',
                  'file': {'@name': 'output.{}'.format(file_ext),
                           '@type': graphics_type.lower()},
                  'graphics': {'legend': {'@kind': 'file',
                                          'file': {'@name': legend_file_name,
                                                   '@type': legend_file_type},
                                          'limited': 'no',
                                          'minimum': '0',
                                          'maximum': '0',
                                          '@type': 'continuous',
                                          'ncolors': str(n_legend_colors),
                                          'nlabels': str(n_legend_labels)},
                               'colortable': 'RAINBOW'},
                  'projection': {'limits': {'@units': 'degrees',
                                            'limit': [{'@role': 'left', '#text': '-180'},
                                                      {'@role': 'right', '#text': '180'},
                                                      {'@role': 'top', '#text': '90'},
                                                      {'@role': 'bottom', '#text': '-90'}]}}}

    return image_info

def make_file(file_uid, result_info):
    ''' Creates structure describing file destination for XML-based task

    Arguments:
        result_info -- dictionary containing type and name of the output file:
            '@file_type' -- type of the file
            '@file_name' -- nama of the file

    Returns:
        file_desc -- dictionary containing XML-based description of a file destination
    '''

    if '@file_name' not in result_info:
        if result_info['@file_type'].lower() == 'netcdf':
            result_info['@file_name'] = 'output.nc'
        else:
            result_info['@file_name'] = 'output.unknown'

    file_desc = {'@uid': file_uid,
                 '@type': 'raw',
                 'file': {'@name': result_info['@file_name'],
                          '@type': result_info['@file_type'].lower()}}

    return file_desc

def make_parameters(proc_options, session, meta):
    ''' Creates modules parameters description for XML task based on JSON argument.

    Arguments:
        proc_options -- JSON-based dictionary describing processing options
        session -- opened session to MDDB
        meta -- MDDB metadata

    Returns:
        parameters -- XML-based dictionary describing module parameters
    '''

    option_tbl = meta.tables['option']
    option_value_tbl = meta.tables['option_value']

    parameters = {'@uid': 'ModuleParameters_1',
                  '@type': 'parameter',
                  'param': []}

    for option in proc_options:
        try:
            name = session.query(option_tbl.c.label).filter(option_tbl.c.id == option['@id']).scalar()
        except NoResultFound:
            logger.error('Can\'t find option_id %s in MDDB table "option"', option['@id'])
            raise
        try:
            value = session.query(option_value_tbl.c.label).filter(option_value_tbl.c.id == option['@value_id']).scalar()
        except NoResultFound:
            logger.error('Can\'t find value_id %s in MDDB table "option_value"', option['@value_id'])
            raise
        param = {'@uid': name,
                 '@type': 'string',
                 '#text': value}
        parameters['param'].append(param)

    return parameters

def make_processing(json_task, session, meta):
    ''' Creates XML processing (along with corresponding data and destinations) description based on JSON argument.

    Arguments:
        json_task -- JSON-based dictionary describing task
        session -- opened session to MDDB
        meta -- MDDB metadata

    Returns:
        data -- list of XML-based dictionaries describing data used in processing
        destinations -- list of XML-based dictionaries describing destinations used in processing
        processing -- list of XML-based dictionaries describing processing conveyor
    '''
    json_proc = json_task['processing']
    if '@position' in json_task:  # Check nested case
        nested_proc_id = json_task['@position']
    else:
        nested_proc_id = 0

    # Map MDDB tables.
    processor_tbl = meta.tables['processor']
    edge_tbl = meta.tables['edge']
    vertex_tbl = meta.tables['vertex']
    from_vertex_tbl = aliased(vertex_tbl)
    to_vertex_tbl = aliased(vertex_tbl)
    data_variable_tbl = meta.tables['data_variable']
    computing_module_tbl = meta.tables['computing_module']

    # Get conveyor id.
    qry = session.query(processor_tbl.c.conveyor_id)
    qry = qry.filter(processor_tbl.c.id == json_proc['@processor_id'])

    try:
        conveyor_id = qry.one()
    except NoResultFound:
        logger.error('No results found in MDDB table "processor" for id %s. No data?', json_proc['@processor_id'])
        raise

    # Get vertices.
    qry = session.query(vertex_tbl.c.id.label('vertex_id'),
                        computing_module_tbl.c.name.label('computing_module'),
                        vertex_tbl.c.condition_option_id.label('condition_option_id'),
                        vertex_tbl.c.condition_value_id.label('condition_value_id'))
    qry = qry.select_from(vertex_tbl)
    qry = qry.join(computing_module_tbl)
    qry = qry.filter(vertex_tbl.c.conveyor_id == conveyor_id)

    try:
        result = qry.all()
    except NoResultFound:
        logger.error('No results found in MDDB table "vertex" for conveyor %s. No data?', conveyor_id)
        raise

    # Put vertices in a graph.
    processing_graph = defaultdict(dict)
    for row in result:
        processing_graph[row.vertex_id]['id'] = row.vertex_id
        processing_graph[row.vertex_id]['module'] = row.computing_module
        processing_graph[row.vertex_id]['condition'] = (row.condition_option_id, row.condition_value_id)
        processing_graph[row.vertex_id]['uplinks'] = []
        processing_graph[row.vertex_id]['downlinks'] = []

    # Get links of vertices.
    qry = session.query(from_vertex_tbl.c.id.label('from_vertex_id'),
                        edge_tbl.c.from_output.label('from_output'),
                        to_vertex_tbl.c.id.label('to_vertex_id'),
                        edge_tbl.c.to_input.label('to_input'),
                        data_variable_tbl.c.label.label('data_label'))
    qry = qry.select_from(edge_tbl)
    qry = qry.join(from_vertex_tbl, and_(edge_tbl.c.from_conveyor_id == from_vertex_tbl.c.conveyor_id,
                                         edge_tbl.c.from_vertex_id == from_vertex_tbl.c.id))
    qry = qry.join(to_vertex_tbl, and_(edge_tbl.c.to_conveyor_id == to_vertex_tbl.c.conveyor_id,
                                       edge_tbl.c.to_vertex_id == to_vertex_tbl.c.id))
    qry = qry.join(data_variable_tbl)
    qry = qry.filter(from_vertex_tbl.c.conveyor_id == conveyor_id)
    qry = qry.filter(to_vertex_tbl.c.conveyor_id == conveyor_id)

    try:
        result = qry.all()
    except NoResultFound:
        logger.error('No results found in MDDB for conveyor %s. Problems in links between tables "edge", "vertex" and "data_variable?', conveyor_id)
        raise

    # Link the vertices and construct the processing graph.
    for row in result:
        downlink = {'output': row.from_output,
                    'vertex': processing_graph[row.to_vertex_id],
                    'data_label': row.data_label}
        processing_graph[row.from_vertex_id]['downlinks'].append(downlink)
        uplink = {'input': row.to_input,
                  'vertex': processing_graph[row.from_vertex_id],
                  'data_label': row.data_label}
        processing_graph[row.to_vertex_id]['uplinks'].append(uplink)

    # Prepare options.
    if 'option' in json_proc:
        options = [(int(opt['@id']), int(opt['@value_id'])) for opt in json_proc['option']]
    else:
        options = []

    # Find conditional vertices that should be deleted.
    vertices_to_delete = []
    for v_num, vertex in processing_graph.items():
        if vertex['condition'] != (None, None) and vertex['condition'] not in options:
            vertices_to_delete.append(v_num)

    # Delete vertices which condition is not met.
    for v_num in vertices_to_delete:
        delete_vertex(v_num, processing_graph)

    data = {}
    processing = {}
    destinations = {}

    # Search for the starting vertex of the graph.
    queue = []
    for vertex in processing_graph.values():
        if vertex['module'] == 'START':
            queue.append(vertex)
            break

    # Add output file info for nested processing case
    if nested_proc_id:
        json_proc['result'] = {}
        json_proc['result']['@file_type'] = 'netcdf'
        json_proc['result']['@file_name'] = 'output_{}.nc'.format(nested_proc_id)

    # Traverse a processing graph using BFS.
    while queue:
        vertex = queue.pop()
        if vertex['module'] != 'START' and vertex['module'] != 'FINISH':  # Ignore start and finish vertices.
            # Create new process description.
            process_id = vertex['id'] - 1
            if process_id in processing.keys() or ('OUTPUT_IMAGE' in [link['data_label'] for link in vertex['downlinks']] and
                                                   nested_proc_id):
                continue
            process = {'@uid': 'Process_{}'.format(process_id),
                       '@class': vertex['module'],
                       'input': [None] * len(set([i['input'] for i in vertex['uplinks']])),
                       'output': [None] * len(set([i['output'] for i in vertex['downlinks']]))}
            # Describe inputs.
            for uplink in vertex['uplinks']:
                input_pos = uplink['input']-1
                uid = 'P{}Input{}'.format(process_id, input_pos+1)
                data_label = uplink['data_label']
                if 'INPUT' in data_label:
                    _, postfix = data_label.split('_')
                    if postfix == 'PARAMETERS':
                        uid = 'P{}Parameters1'.format(process_id)
                        data_label = 'ModuleParameters_1'  # Disabled for a while: '{}'.format(process_id)
                        if data_label not in data.keys():  # Add modules parameters.
                            data[data_label] = make_parameters(json_proc['option'], session, meta)
                    else:
                        data_label = 'Data_{}'.format(postfix)
                process['input'][input_pos] = {'@uid': uid, '@data': data_label}
            # Describe outputs.
            for downlink in vertex['downlinks']:
                output_pos = downlink['output']-1
                uid = 'P{}Output{}'.format(process_id, output_pos+1)
                data_label = downlink['data_label']
                if 'OUTPUT' in data_label and data_label not in destinations.keys():
                    _, postfix = data_label.split('_')
                    if postfix == 'IMAGE':
                        destinations[data_label] = make_image(data_label, json_proc['result']['@graphics_type'])
                    if postfix == 'RAW':
                        destinations[data_label] = make_file(data_label, json_proc['result'])
                if 'RESULT' in data_label and data_label not in data.keys():
                    _, postfix = data_label.split('_')
                    if postfix == 'TREND':
                        title = 'Trend stub title'
                        name = 'Trend of stub name'
                        units = 'Stub units / 10yrs'
                    else:
                        title = 'Stub title'
                        name = 'Stub name'
                        units = 'Stub units'
                    data[data_label] = make_data_array(data_label, title, name, units)
                process['output'][output_pos] = {'@uid': uid, '@data': data_label}
            processing[process_id] = process
        for to_link in vertex['downlinks']:
            queue.append(to_link['vertex'])

    data = [val for val in data.values()]
    destinations = [val for val in destinations.values()]
    processing = [processing[i] for i in range(1, max(processing.keys())+1) if i in processing.keys()]

    return data, destinations, processing

def make_data_file(nested_task):
    ''' Creates XML data file description for corresponding nested processings.
    Nested processings stores results in intermediate netCDF files.
    This function generates descriptions of these files so they can be read by the main processing.

    Arguments:
        nested_task -- nested task

    Returns:
        data_file -- XML-based dictionary describing data file.
    '''

    data_file = {}
    _, pos = nested_task['task']['@uid'].split('_')
    data_file['@uid'] = 'Data_{}'.format(pos)
    data_file['@type'] = copy(nested_task['task']['destination'][0]['@type'])
    data_file['file'] = deepcopy(nested_task['task']['destination'][0]['file'])
    data_file['variable'] = {}
    data_file['variable']['@name'] = 'data'
    data_file['region'] = deepcopy(nested_task['task']['data'][0]['region'])
    data_file['levels'] = deepcopy(nested_task['task']['data'][0]['levels'])
    data_file['time'] = {}
    data_file['time']['@template'] = copy(nested_task['task']['data'][0]['time']['@template'])
    data_file['time']['segment'] = {}
    data_file['time']['segment']['@beginning'] = copy(nested_task['task']['data'][0]['time']['segment'][0]['@beginning'])
    data_file['time']['segment']['@ending'] = copy(nested_task['task']['data'][0]['time']['segment'][-1]['@ending'])
    data_file['time']['segment']['@name'] = 'GlobalSeg'

    return data_file

def task_generator(json_task, task_id, metadb_info):
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

    logger.info('Generating XML tasks')
    # Connect to metadata DB
    db_url = '{0}://{1}@{2}/{3}'.format(metadb_info['engine'], metadb_info['user'], metadb_info['host'], metadb_info['name'])
    engine = create_engine(db_url)
    meta = MetaData()
    meta.reflect(bind=engine)
    session_class = sessionmaker(bind=engine)
    session = session_class()

    # All tasks list
    all_tasks = []

    # Create a basic task
    current_task = {}
    current_task['@uid'] = str(task_id)
    if '@position' in json_task:  # Check nested case
        current_task['@uid'] += '_' + json_task['@position']
    current_task['metadb'] = {'@host': metadb_info['host'],
                              '@name': metadb_info['name'],
                              '@user': metadb_info['user'],
                              '@password': metadb_info['password']}
    current_task['data'] = []

    if not isinstance(json_task['processing']['argument'], list):
        json_task['processing']['argument'] = [json_task['processing']['argument']]
    for arg in json_task['processing']['argument']:
        if 'data' in arg.keys():
            current_task['data'].append(make_data_arguments(arg, session, meta))
        elif 'processing' in arg.keys():
            nested_tasks = task_generator(arg, task_id, metadb_info)
            current_task['data'].append(make_data_file(nested_tasks[0]))
            all_tasks.extend(nested_tasks)

    data, destinations, processing = make_processing(json_task, session, meta)

    current_task['data'].extend(data)
    current_task['destination'] = destinations
    current_task['processing'] = processing

    if len(all_tasks) > 0:
        all_tasks.append({'wait': True})  # Append wait signal after nested tasks.
    all_tasks.append({'task': current_task})

    return all_tasks

if __name__ == "__main__":
    core_config = ConfigParser()
    core_config.read(os.path.join(str(os.path.dirname(__file__)), 'core_config.ini'))
#    JSON_FILE_NAME = '..\\very_simple_task.json'
#    JSON_FILE_NAME = '..\\simple_task.json'
#    JSON_FILE_NAME = '..\\complex_task.json'
    JSON_FILE_NAME = '..\\CalcMonthMaxMax.json'
    with open(JSON_FILE_NAME, 'r') as json_file:
        j_task = json.load(json_file)
    tasks = task_generator(j_task, 1, core_config['METADB'])
    i = 0
    for task in tasks:
        if 'wait' in task:
            continue
        i += 1
        XML_FILE_NAME = '..\\output_task_{}.xml'.format(i)
        with open(XML_FILE_NAME, 'w') as xml_file:
            xmltodict.unparse(task, xml_file, pretty=True)
