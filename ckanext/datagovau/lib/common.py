import os
import time

import ckan.logic as logic
import ckan.plugins.toolkit as toolkit
from ckanext.datapusher.logic.action import datapusher_status

_get_or_bust = logic.get_or_bust


def _delete_resource(context, ckan, resource, msg_prefix, task_type):
    try:
        update_cleanup_status(context,
                              'pending',
                              "{0} deleting child resource {1}".format(msg_prefix, resource['name']),
                              resource,
                              task_type,
                              None)
        api_res = ckan.call_action('resource_delete', data_dict={'id': resource['id']})

        if context['wait_for_datapusher'] and task_type == 'zipextractor' and resource.get('format', '').upper() in \
                context['datapusher_default_formats']:
            update_cleanup_status(context,
                                  'pending',
                                  "{0} waiting for datapusher to ingest {1}...".format(msg_prefix, resource['name']),
                                  resource,
                                  task_type,
                                  None)

            # Datapusher will be called on this resource
            status = 'submitting'
            counter = 0
            while status not in ['error', 'complete']:
                try:
                    status = datapusher_status(context, resource)['status']
                except Exception:
                    update_cleanup_status(context,
                                          'error',
                                          "{0} {1} caused a datapusher error, continuing...".format(msg_prefix,
                                                                                                    resource['name']),
                                          resource,
                                          task_type,
                                          None)
                    break

                time.sleep(1)
                counter += 1

                if (status == 'submitting' and counter >= context['datapusher_submit_timeout']) or \
                        (status != 'submitting' and counter >= context['datapusher_completion_timeout']):
                    update_cleanup_status(context,
                                          'error',
                                          "{0} {1} caused a datapusher timeout, continuing...".format(msg_prefix,
                                                                                                      resource['name']),
                                          resource,
                                          task_type,
                                          None)
                    break

            else:
                update_cleanup_status(context,
                                      'pending',
                                      "{0} datapusher finished ingesting {1}, continuing...".format(msg_prefix,
                                                                                                    resource['name']),
                                      resource,
                                      task_type,
                                      None)

    except Exception, e:
        update_cleanup_status(context,
                              'error',
                              "{0} failed to delete child resource {1}, continuing...".format(
                                  msg_prefix,
                                  resource['name']),
                              resource,
                              task_type,
                              "{0} failed to delete child resource {1} with exception {2}, continuing...".format(
                                  msg_prefix,
                                  resource['name'],
                                  str(e)))

    update_cleanup_status(context,
                          'pending',
                          "{0} child resource {1} deleted.".format(msg_prefix, resource['name']),
                          resource,
                          task_type,
                          None)


def delete_children(context, ckan, resource, child_key, msg_prefix, msg_suffix, task_type):
    def delete_child_recurse(r_res, pkg):
        for res in pkg['resources']:
            if res.get(child_key, '') == r_res['id']:
                delete_child_recurse(res, pkg)
                _delete_resource(context, ckan, res, msg_prefix, task_type)

    try:
        update_cleanup_status(context,
                              'pending',
                              "{0} retrieving package data containing resource {1}.".format(msg_prefix,
                                                                                            resource.get('name',
                                                                                                         resource[
                                                                                                             'id'])),
                              resource,
                              task_type,
                              None)
        package = ckan.call_action('package_show', data_dict={'id': resource['package_id']})
    except logic.NotFound:
        update_cleanup_status(context,
                              'pending',
                              "{0} package ID: {1} already deleted.".format(msg_prefix, resource['package_id']),
                              resource,
                              task_type,
                              None)
        return
    except Exception, e:
        update_cleanup_status(context,
                              'error',
                              "{0} failed to retrieve package ID: {1}, {2}".format(msg_prefix, resource['package_id'],
                                                                                   msg_suffix),
                              resource,
                              task_type,
                              "{0} failed to retrieve package ID: {1} with error {2}, {3}".format(msg_prefix,
                                                                                                  resource[
                                                                                                      'package_id'],
                                                                                                  str(e), msg_suffix))
        return

    for res in package['resources']:
        if res.get(child_key, '') == resource['id']:
            # Update resource about to be deleted to not trigger the notify
            import ckan.model as model
            resource['state'] = model.Resource.get(resource['id']).state

            update_cleanup_status(context,
                                  'error',
                                  "{0} retrieved state for {1}: {2}".format(msg_prefix, resource['name'],
                                                                            resource['state']),
                                  resource,
                                  task_type,
                                  None)

            if resource['state'] != 'deleted' and resource.get('zip_delete_trigger', 'True') == 'True':
                update_res = ckan.call_action('resource_show', data_dict={'id': resource['id']})
                update_res['zip_delete_trigger'] = 'False'
                update_res = ckan.call_action('resource_update', data_dict=update_res)
                resource['zip_delete_trigger'] = update_res['zip_delete_trigger']
            delete_child_recurse(res, package)
            _delete_resource(context, ckan, res, msg_prefix, task_type)


def delete_all_children(context, ckan, package, child_key, msg_prefix, task_type):
    for res in package['resources']:
        if child_key in res:
            _delete_resource(context, ckan, res, msg_prefix, task_type)


def update_process_status(context, current_status, msg, resource, task_type, admin_msg=None):
    toolkit.get_action(task_type + '_hook')(
        context, {
            'status': current_status,
            'resource': resource,
            'msg': msg,
            'task_type': 'process',
            'admin_msg': '' if admin_msg is None else admin_msg
        })


def update_cleanup_status(context, current_status, msg, resource, task_type, admin_msg=None):
    toolkit.get_action(task_type + '_hook')(
        context, {
            'status': current_status,
            'resource': resource,
            'msg': msg,
            'task_type': 'cleanup',
            'admin_msg': '' if admin_msg is None else admin_msg
        })


def _load_config(context):
    import paste.deploy
    config_abs_path = context['config_file_path']
    conf = paste.deploy.appconfig('config:' + config_abs_path)
    import ckan
    ckan.config.environment.load_environment(conf.global_conf,
                                             conf.local_conf)


def _register_translator():
    # Register a translator in this thread so that
    # the _() functions in logic layer can work
    from paste.registry import Registry
    from pylons import translator
    from ckan.lib.cli import MockTranslator
    global registry
    registry = Registry()
    registry.prepare()
    global translator_obj
    translator_obj = MockTranslator()
    registry.register(translator, translator_obj)


def init(raw_context, resource, task_type):
    _load_config(raw_context)
    _register_translator()

    from ckan import model

    # Complete the context
    raw_context['model'] = model
    raw_context['session'] = model.Session

    if not os.path.isdir(raw_context['temporary_directory']):
        try:
            os.makedirs(raw_context['temporary_directory'])
        except:
            update_process_status(raw_context,
                                  'error',
                                  "Failed to create temporary directory, skipping...",
                                  resource['id'],
                                  task_type,
                                  "Failed to create temporary directory {0}, skipping...".format(
                                      raw_context['temporary_directory']))
            return None

    return raw_context
