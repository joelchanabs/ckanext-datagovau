import ckan.plugins as plugins
import ckan.lib as lib
import ckan.plugins.toolkit as tk
import ckan.model as model
import ckan.logic as logic
import ckanext.datastore.db as datastore_db
import os, time, requests

import ckanext.datagovau.action as action
from ckan.lib.plugins import DefaultOrganizationForm
from ckan.lib import uploader, formatters
import feedparser


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
    stats = {}
    stats['dataset_count'] = logic.get_action('package_search')({}, {"rows": 1})['count']
    stats['group_count'] = len(logic.get_action('group_list')({}, {}))
    stats['organization_count'] = len(logic.get_action('organization_list')({}, {}))

    stats['unpub_data_count'] = 0
    for fDict in \
    logic.get_action('package_search')({}, {"facet.field": ["unpublished"], "rows": 1})['search_facets']['unpublished'][
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
    plugins.implements(plugins.IConfigurer, inherit=False)
    plugins.implements(plugins.ITemplateHelpers, inherit=False)
    plugins.implements(plugins.IActions, inherit=True)
    plugins.implements(plugins.IPackageController, inherit=True)
    plugins.implements(plugins.IFacets, inherit=True)

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

        # config['licenses_group_url'] = 'http://%(ckan.site_url)/licenses.json'

    def get_helpers(self):
        return {'get_user_datasets': get_user_datasets, 'get_user_datasets_public': get_user_datasets_public,
                'get_ddg_site_statistics': get_ddg_site_statistics, 'get_resource_file_size': get_resource_file_size,
                'blogfeed': blogfeed}

    # IActions

    def get_actions(self):
        return {'group_tree': action.group_tree,
                'group_tree_section': action.group_tree_section,
                }


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
