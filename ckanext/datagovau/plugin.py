import ckan.plugins as plugins
import ckan.lib as lib
import ckan.lib.cli as cli
from ckan.lib.celery_app import celery
import ckan.plugins.toolkit as tk
import ckan.model as model
from ckan.model.domain_object import DomainObjectOperation
import ckan.logic as logic
import ckanext.datastore.db as datastore_db
import os, time, uuid

import ckanext.datagovau.action as action
from pylons import config
from ckan.lib.plugins import DefaultOrganizationForm
from ckan.lib import uploader, formatters
import feedparser

import logging

log = logging.getLogger('ckanext_datagovau')


# get user created datasets and those they have edited
def get_user_datasets(user_dict):
    context = {'model': model, 'user': user_dict['name']}
    created_datasets_list = user_dict['datasets']
    active_datasets_list = [logic.get_action('package_show')(context, {'id': x['data']['package']['id']}) for x in
                            lib.helpers.get_action('user_activity_list', {'id': user_dict['id']}) if
                            x['data'].get('package')]
    raw_list = sorted(active_datasets_list + created_datasets_list, key=lambda pkg: pkg['state'])
    filtered_dict = {}
    for dataset in raw_list:
        if dataset['id'] not in filtered_dict.keys():
            filtered_dict[dataset['id']] = dataset
    return filtered_dict.values()


def get_user_datasets_public(user_dict):
    return [pkg for pkg in get_user_datasets(user_dict) if pkg['state'] == 'active']


def get_ddg_site_statistics():
    def fetch_ddg_stats():
        stats = {'dataset_count': logic.get_action('package_search')({}, {"rows": 1})['count'],
                 'group_count': len(logic.get_action('group_list')({}, {})),
                 'organization_count': len(logic.get_action('organization_list')({}, {})), 'unpub_data_count': 0}

        for fDict in \
                logic.get_action('package_search')({}, {"facet.field": ["unpublished"], "rows": 1})['search_facets'][
                    'unpublished'][
                    'items']:
            if fDict['name'] == "Unpublished datasets":
                stats['unpub_data_count'] = fDict['count']
                break

        result = model.Session.execute(
            '''select count(*) from related r
               left join related_dataset rd on r.id = rd.related_id
               where rd.status = 'active' or rd.id is null''').first()[0]
        stats['related_count'] = result

        stats['open_count'] = logic.get_action('package_search')({}, {"fq": "isopen:true", "rows": 1})['count']

        stats['api_count'] = logic.get_action('resource_search')({}, {"query": ["format:wms"]})['count'] + len(
            datastore_db.get_all_resources_ids_in_datastore())

        return stats

    if tk.asbool(config.get('ckanext.stats.cache_enabled', 'True')):
        from pylons import cache

        key = 'ddg_site_stats'
        res_stats = cache.get_cache('ddg_ext', type='dbm').get_value(key=key,
                                                                     createfunc=fetch_ddg_stats,
                                                                     expiretime=tk.asint(
                                                                         config.get('ckanext.stats.cache_fast_timeout',
                                                                                    '600')))
    else:
        res_stats = fetch_ddg_stats()

    return res_stats


def get_resource_file_size(rsc):
    if rsc.get('url_type') == 'upload':
        upload = uploader.ResourceUpload(rsc)
        value = None
        try:
            value = os.path.getsize(upload.get_path(rsc['id']))
            value = formatters.localised_filesize(int(value))
        except Exception:
            # Sometimes values that can't be converted to ints can sneak
            # into the db. In this case, just leave them as they are.
            pass
        return value
    return None


def blogfeed():
    d = feedparser.parse('https://blog.data.gov.au/blogs/rss.xml')
    for entry in d.entries:
        entry.date = time.strftime("%a, %d %b %Y", entry.published_parsed)
    return d


class DataGovAuPlugin(plugins.SingletonPlugin,
                      tk.DefaultDatasetForm):
    '''An example IDatasetForm CKAN plugin.

    Uses a tag vocabulary to add a custom metadata field to datasets.

    '''
    plugins.implements(plugins.IConfigurable, inherit=True)
    plugins.implements(plugins.IConfigurer, inherit=False)
    plugins.implements(plugins.ITemplateHelpers, inherit=False)
    plugins.implements(plugins.IActions, inherit=True)
    plugins.implements(plugins.IPackageController, inherit=True)
    plugins.implements(plugins.IFacets, inherit=True)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)

    def configure(self, config):
        core_url = config.get('ckan.site_url', 'http://localhost:8000/')
        self.context = {'user': model.User.get(config.get('ckan.dataingestor.ckan_user', 'default')).name,
                        'postgis': cli.parse_db_config('ckan.dataingestor.postgis_url'),
                        'geoserver': cli.parse_db_config('ckan.dataingestor.geoserver_url'),
                        'geoserver_public_url': config.get('ckan.dataingestor.public_geoserver',
                                                           core_url + '/geoserver'),
                        'org_blacklist': list(
                            set(tk.aslist(config.get('ckan.dataingestor.spatial.org_blacklist', [])))),
                        'pkg_blacklist': list(
                            set(tk.aslist(config.get('ckan.dataingestor.spatial.pkg_blacklist', [])))),
                        'user_blacklist': list(set(map(lambda x: model.User.get(x).id,
                                                       tk.aslist(
                                                           config.get('ckan.dataingestor.spatial.user_blacklist',
                                                                      []))))),
                        'target_spatial_formats': list(set(map(lambda x: x.upper(),
                                                               tk.aslist(
                                                                   config.get(
                                                                       'ckan.dataingestor.spatial.target_formats',
                                                                       []))))),
                        'target_zip_formats': list(set(map(lambda x: x.upper(),
                                                           tk.aslist(
                                                               config.get('ckan.dataingestor.zip.target_formats',
                                                                          []))))),
                        'temporary_directory': config.get('ckan.dataingestor.temporary_directory', '/tmp/ckan_ingest'),
                        'config_file_path': os.path.abspath(config['__file__'])}


    def dataset_facets(self, facets, package_type):
        if 'jurisdiction' in facets:
            facets['jurisdiction'] = 'Jurisdiction'
        if 'unpublished' in facets:
            facets['unpublished'] = 'Published Status'
        return facets

    def before_search(self, search_params):
        """
        IPackageController::before_search.

        Add default sorting to package_search.
        """
        if 'sort' not in search_params:
            search_params['sort'] = 'extras_harvest_portal asc, score desc, metadata_modified desc'
        return search_params

    def after_search(self, search_results, data_dict):
        if 'unpublished' in search_results['facets']:
            search_results['facets']['unpublished']['Published datasets'] = search_results['count'] - \
                                                                            search_results['facets']['unpublished'].get(
                                                                                'True', 0)
            if 'True' in search_results['facets']['unpublished']:
                search_results['facets']['unpublished']['Unpublished datasets'] = \
                    search_results['facets']['unpublished']['True']
                del search_results['facets']['unpublished']['True']
            restructured_facet = {
                'title': 'unpublished',
                'items': []
            }
            for key_, value_ in search_results['facets']['unpublished'].items():
                new_facet_dict = {}
                new_facet_dict['name'] = key_
                new_facet_dict['display_name'] = key_
                new_facet_dict['count'] = value_
                restructured_facet['items'].append(new_facet_dict)
            search_results['search_facets']['unpublished'] = restructured_facet

        return search_results

    def update_config(self, config):
        # Add this plugin's templates dir to CKAN's extra_template_paths, so
        # that CKAN will use this plugin's custom templates.
        # here = os.path.dirname(__file__)
        # rootdir = os.path.dirname(os.path.dirname(here))

        tk.add_template_directory(config, 'templates')
        tk.add_public_directory(config, 'theme/public')
        tk.add_resource('theme/public', 'ckanext-datagovau')
        tk.add_resource('public/scripts/vendor/jstree', 'jstree')

    def get_helpers(self):
        return {'get_user_datasets': get_user_datasets,
                'get_user_datasets_public': get_user_datasets_public,
                'get_ddg_site_statistics': get_ddg_site_statistics,
                'get_resource_file_size': get_resource_file_size,
                'blogfeed': blogfeed}

    # IActions

    def get_actions(self):
        return {'group_tree': action.group_tree,
                'group_tree_section': action.group_tree_section}

    # IDomainObjectModification

    def notify(self, entity, operation=None):
        if isinstance(entity, model.Resource):
            # new event is sent, then a changed event.
            log.warn('Operation of type {0} detected!'.format(operation))
            # There is a NEW or CHANGED resource. We will send a task to celery
            # to analyze the package

            # Very ugly. Unfortunately, for resources UI deletions are registered as
            # a 'change' and the entity itself does not have a 'state' associated with it.
            # The best option I've found is to query for the ID and if it isn't found,
            # it is then safe to assume it is deleted.
            resource = None
            try:
                resource = tk.get_action('resource_show')({'ignore_auth': True, 'user': self.context['user']}, {'id': entity.id})
            except tk.ObjectNotFound:
                # There is a resource which is being deleted
                celery.send_task(
                    'datagovau.delete_children',
                    args=[self.context, entity.as_dict()],
                    task_id='{}-{}'.format(str(uuid.uuid4()), operation))

            if resource is not None:
                celery.send_task(
                    'datagovau.spatial_ingest',
                    args=[self.context, resource],
                    task_id='{}-{}'.format(str(uuid.uuid4()), operation))
                celery.send_task(
                    'datagovau.zip_extract',
                    args=[self.context, resource],
                    task_id='{}-{}'.format(str(uuid.uuid4()), operation))


class HierarchyForm(plugins.SingletonPlugin, DefaultOrganizationForm):
    plugins.implements(plugins.IGroupForm, inherit=True)

    # IGroupForm

    def group_types(self):
        return ('organization',)

    def setup_template_variables(self, context, data_dict):
        from pylons import tmpl_context as c

        model = context['model']
        group_id = data_dict.get('id')
        if group_id:
            group = model.Group.get(group_id)
            c.allowable_parent_groups = \
                group.groups_allowed_to_be_its_parent(type='organization')
        else:
            c.allowable_parent_groups = model.Group.all(
                group_type='organization')
