import datetime
import json

import ckan.lib.navl.dictization_functions
import ckan.logic as logic
import ckan.plugins.toolkit as toolkit
from ckan.common import _
from ckan.lib.celery_app import celery
from ckan.model.types import make_uuid
from dateutil.parser import parse as parse_date

import ckanext.datagovau.logic.schema as ddgschema
from ckanext.datagovau.helpers import log, get_spatial_context, get_zip_context
from ckanext.datagovau.model import GroupTreeNode

_get_or_bust = logic.get_or_bust
_validate = ckan.lib.navl.dictization_functions.validate


@logic.side_effect_free
def group_tree(context, data_dict):
    '''Returns the full group tree hierarchy.

    :returns: list of top-level GroupTreeNodes
    '''
    model = _get_or_bust(context, 'model')
    group_type = data_dict.get('type', 'group')
    return [_group_tree_branch(group, type=group_type)
            for group in model.Group.get_top_level_groups(type=group_type)]


@logic.side_effect_free
def group_tree_section(context, data_dict):
    '''Returns the section of the group tree hierarchy which includes the given
    group, from the top-level group downwards.

    :param id: the id or name of the group to inclue in the tree
    :returns: the top GroupTreeNode of the tree section
    '''
    group_name_or_id = _get_or_bust(data_dict, 'id')
    model = _get_or_bust(context, 'model')
    group = model.Group.get(group_name_or_id)
    if group is None:
        raise toolkit.ObjectNotFound
    group_type = data_dict.get('type', 'group')
    if group.type != group_type:
        how_type_was_set = 'was specified' if data_dict.get('type') \
            else 'is filtered by default'
        raise toolkit.ValidationError(
            'Group type is "%s" not "%s" that %s' %
            (group.type, group_type, how_type_was_set))
    root_group = (group.get_parent_group_hierarchy(type=group_type) or [group])[0]
    return _group_tree_branch(root_group, highlight_group_name=group.name,
                              type=group_type)


def _group_tree_branch(root_group, highlight_group_name=None, type='group'):
    '''Returns a branch of the group tree hierarchy, rooted in the given group.

    :param root_group_id: group object at the top of the part of the tree
    :param highlight_group_name: group name that is to be flagged 'highlighted'
    :returns: the top GroupTreeNode of the tree
    '''
    nodes = {}  # group_id: GroupTreeNode()
    root_node = nodes[root_group.id] = GroupTreeNode(
        {'id': root_group.id,
         'name': root_group.name,
         'title': root_group.title})
    if root_group.name == highlight_group_name:
        nodes[root_group.id].highlight()
        highlight_group_name = None
    for group_id, group_name, group_title, parent_id in \
            root_group.get_children_group_hierarchy(type=type):
        node = GroupTreeNode({'id': group_id,
                              'name': group_name,
                              'title': group_title})
        nodes[parent_id].add_child_node(node)
        if highlight_group_name and group_name == highlight_group_name:
            node.highlight()
        nodes[group_id] = node
    return root_node


def _ingestor_submit(context, resource, task_key, celery_process):
    msg = 'Task submitted.'
    log.info(msg)

    toolkit.check_access(task_key + '_submit', context, resource)

    current_time = str(datetime.datetime.utcnow())

    task = {
        'entity_id': resource['id'],
        'entity_type': 'resource',
        'task_type': 'process',
        'last_updated': current_time,
        'key': task_key,
        'error': '',
        'state': 'pending',
        'value': json.dumps({
            'original_url': resource['url'],
            'created': current_time,
            'log': [{
                'level': 'INFO',
                'timestamp': current_time,
                'message': msg,
                'admin_message': msg
            }, ]})
    }

    try:
        task_id = toolkit.get_action('task_status_show')(context, {
            'entity_id': resource['id'],
            'task_type': 'cleanup',
            'key': task_key
        })['id']
    except logic.NotFound:
        task_id = make_uuid()

    if task_key == 'zipextractor':
        ser_context = get_zip_context()
    else:
        ser_context = get_spatial_context()

    ser_context['user'] = context['user']

    # Store task in context variable as recalling the persisted value does not
    # sufficiently keep up with fast updates. This is most likely due to cache
    # lag with Solr queries
    ser_context['task'] = task

    ser_context = json.dumps(ser_context)

    data = json.dumps(resource)

    celery.send_task(celery_process, args=[ser_context, data], task_id=task_id)

    context['ignore_auth'] = True
    toolkit.get_action('task_status_update')(context, task)


def _ingestor_cleanup(context, resource, task_key, celery_process):
    msg = 'Cleanup task submitted.'
    log.info(msg)

    toolkit.check_access(task_key + '_cleanup', context, resource)

    current_time = str(datetime.datetime.utcnow())

    task = {
        'entity_id': resource['id'],
        'entity_type': 'resource',
        'task_type': 'cleanup',
        'last_updated': current_time,
        'state': 'pending',
        'key': task_key,
        'error': '',
        'value': json.dumps({
            'original_url': resource.get('url', ''),
            'created': current_time,
            'log': [{
                'level': 'INFO',
                'timestamp': current_time,
                'message': msg,
                'admin_message': msg
            }, ]})
    }

    try:
        task_id = toolkit.get_action('task_status_show')(context, {
            'entity_id': resource['id'],
            'task_type': 'cleanup',
            'key': task_key
        })['id']
    except logic.NotFound:
        task_id = make_uuid()

    if task_key == 'zipextractor':
        ser_context = get_zip_context()
    else:
        ser_context = get_spatial_context()

    ser_context['user'] = context['user']

    # Store task in context variable as recalling the persisted value does not
    # sufficiently keep up with fast updates. This is most likely due to cache
    # lag with Solr queries
    ser_context['task'] = task

    ser_context = json.dumps(ser_context)

    data = json.dumps(resource)

    celery.send_task(celery_process, args=[ser_context, data], task_id=task_id)

    context['ignore_auth'] = True
    toolkit.get_action('task_status_update')(context, task)


def _ingestor_hook(context, resource, status, msg, admin_msg, task_key, task_type):
    if status == 'error':
        log.error(msg)
    else:
        log.info(msg)

    # Store task in context variable as recalling the persisted value does not
    # sufficiently keep up with fast updates. This is most likely due to cache
    # lag with Solr queries
    current_time = str(datetime.datetime.utcnow())
    task = context.get('task', {
        'entity_id': resource['id'],
        'entity_type': 'resource',
        'task_type': task_type,
        'last_updated': current_time,
        'state': 'pending',
        'key': task_key,
        'error': '',
        'value': json.dumps({
            'original_url': resource.get('url', ''),
            'created': current_time,
            'log': [{
                'level': 'INFO',
                'timestamp': current_time,
                'message': 'Cleanup task submitted.',
                'admin_message': 'Cleanup task submitted.'
            }, ]})
    })

    update = True
    try:
        task['id'] = toolkit.get_action('task_status_show')(context, {
            'entity_id': resource['id'],
            'task_type': task_type,
            'key': task_key
        })['id']
    except logic.NotFound:
        update = False
        # Most likely called from paster command thread
        pass

    task_value = json.loads(task['value'])
    task_value['log'].append({
        'level': 'ERROR' if status == 'error' else 'INFO',
        'timestamp': task['last_updated'],
        'message': msg,
        'admin_message': msg if admin_msg == '' else admin_msg
    })
    task_error = ''
    if status == 'error':
        task_value['error'] = msg
        task_error = msg

    task_value = json.dumps(task_value)

    task['last_updated'] = str(datetime.datetime.utcnow())
    task['state'] = status
    task['error'] = task_error
    task['value'] = task_value

    context['task'] = task

    resubmit = False

    if status == 'complete':
        # Create default views for resource if necessary (only the ones that
        # require data to be in the DataStore)
        try:
            resource_dict = toolkit.get_action('resource_show')(
                context, {'id': resource['id']})
        except:
            # Resource must have been deleted. So, we don't need to do anything else.
            return

        # Check if the uploaded file has been modified in the meantime
        task_created = json.loads(task_value).get('created')
        if resource_dict.get('last_modified') and task_created:
            try:
                last_modified_datetime = parse_date(resource_dict['last_modified'])
                task_created_datetime = parse_date(task_created)
                if last_modified_datetime > task_created_datetime:
                    log.debug('Uploaded file more recent: {0} > {1}'.format(
                        last_modified_datetime, task_created_datetime))
                    resubmit = True
            except ValueError:
                pass

    if update:
        context['ignore_auth'] = True
        toolkit.get_action('task_status_update')(context, task)

        if resubmit:
            log.debug(
                '{0} has been modified, resubmitting ({1})...'.format(resource.get('name', resource['id']), task_key))
            toolkit.get_action(task_key + '_submit')(context, resource)


def _ingestor_status(context, resource, task_key, task_type):
    toolkit.check_access(task_key + '_status', context, {
        'id': resource['id']
    })

    task = toolkit.get_action('task_status_show')(context, {
        'entity_id': resource['id'],
        'task_type': task_type,
        'key': task_key
    })

    return {
        'status': task['state'],
        'last_updated': task['last_updated'],
        'task_info': json.loads(task['value'])
    }


def zipextractor_submit(context, resource):
    if resource.get('name', '') == '':
        resource['name'] = _('Unamed resource')

    schema = context.get('schema', ddgschema.zipextractor_submit_schema())
    data_dict, errors = _validate(resource, schema, context)
    if errors:
        raise toolkit.ValidationError(errors)

    _ingestor_submit(context, data_dict, 'zipextractor', 'datagovau.zip_extract')


def zipextractor_cleanup(context, resource):
    schema = context.get('schema', ddgschema.zipextractor_cleanup_schema())
    data_dict, errors = _validate(resource, schema, context)
    if errors:
        raise toolkit.ValidationError(errors)

    _ingestor_cleanup(context, data_dict, 'zipextractor', 'datagovau.zip_cleanup')


def zipextractor_hook(context, data_dict):
    ''' Update zipextractor task. This action is typically called by the
    zipextractor whenever the status of a job changes.

    :param status: status of the job from the zipextractor service
    :type status: string
    :param resource: resource id
    :type resource: string
    :param task_info: message list of task steps
    :type tast_info: list[string]
    '''

    resource, status, msg, task_type, admin_msg = _get_or_bust(data_dict,
                                                               ['resource', 'status', 'msg', 'task_type', 'admin_msg'])
    _ingestor_hook(context, resource, status, msg, admin_msg, 'zipextractor', task_type)


def zipextractor_status(context, resource):
    return _ingestor_status(context, resource, 'zipextractor', 'process')


def spatialingestor_submit(context, resource):
    if resource.get('name', '') == '':
        resource['name'] = _('Unamed resource')

    schema = context.get('schema', ddgschema.spatialingestor_submit_schema())
    data_dict, errors = _validate(resource, schema, context)
    if errors:
        raise toolkit.ValidationError(errors)

    _ingestor_submit(context, data_dict, 'spatialingestor', 'datagovau.spatial_ingest')


def spatialingestor_cleanup(context, data_dict):
    schema = context.get('schema', ddgschema.spatialingestor_cleanup_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise toolkit.ValidationError(errors)

    _ingestor_cleanup(context, data_dict, 'spatialingestor', 'datagovau.spatial_cleanup')


def spatialingestor_hook(context, data_dict):
    ''' Update spatialingestor task. This action is typically called by the
    spatialingestor whenever the status of a job changes.

    :param status: status of the job from the spatialingestor service
    :type status: string
    :param resource: resource dict
    :type resource: dict
    :param task_info: message list of task steps
    :type tast_info: list[string]
    '''

    resource, status, msg, task_type, admin_msg = _get_or_bust(data_dict,
                                                               ['resource', 'status', 'msg', 'task_type', 'admin_msg'])
    _ingestor_hook(context, resource, status, msg, admin_msg, 'spatialingestor', task_type)


def spatialingestor_status(context, resource):
    return _ingestor_status(context, resource, 'spatialingestor', 'process')
