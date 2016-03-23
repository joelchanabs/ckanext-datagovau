import logging

import ckan.plugins as plugins
import ckan.lib as lib
import ckan.lib.dictization.model_dictize as model_dictize
import ckan.plugins.toolkit as tk
import ckan.model as model
import ckan.logic as logic
from pylons import config

from sqlalchemy import orm
import ckan.model

import ckanext.datagovau.action as action
from ckan.lib.plugins import DefaultGroupForm
from ckan.lib.plugins import DefaultOrganizationForm
# get user created datasets and those they have edited
def get_user_datasets(user_dict):
    created_datasets_list = user_dict['datasets']
    active_datasets_list = [x['data']['package'] for x in
                            lib.helpers.get_action('user_activity_list', {'id': user_dict['id']}) if
                            x['data'].get('package')]
    raw_list = created_datasets_list + active_datasets_list
    filtered_dict = {}
    for dataset in raw_list:
        if dataset['id'] not in filtered_dict.keys():
            filtered_dict[dataset['id']] = dataset
    return filtered_dict.values()

def get_ddg_site_statistics():
    stats = {}
    result = model.Session.execute("select count(*) from package where package.state='active' "
                                   "and package.type ='dataset' and package.private = 'f' ").first()[0]
    stats['dataset_count'] = result
    stats['group_count'] = len(logic.get_action('group_list')({}, {}))
    stats['organization_count'] = len(
        logic.get_action('organization_list')({}, {}))

    return stats


class DataGovAuPlugin(plugins.SingletonPlugin,
                      tk.DefaultDatasetForm):
    '''An example IDatasetForm CKAN plugin.

    Uses a tag vocabulary to add a custom metadata field to datasets.

    '''
    plugins.implements(plugins.IConfigurer, inherit=False)
    plugins.implements(plugins.ITemplateHelpers, inherit=False)
    plugins.implements(plugins.IActions, inherit=True)

    def update_config(self, config):
        # Add this plugin's templates dir to CKAN's extra_template_paths, so
        # that CKAN will use this plugin's custom templates.

        tk.add_template_directory(config, 'templates')
        tk.add_public_directory(config, 'theme/public')
        tk.add_resource('theme/public', 'ckanext-datagovau')
        tk.add_resource('public/scripts/vendor/jstree', 'jstree')

    def get_helpers(self):
        return {'get_user_datasets': get_user_datasets,
                'get_ddg_site_statistics': get_ddg_site_statistics}

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