import logging

import ckan.plugins as plugins
import ckan.lib as lib
import ckan.lib.dictization.model_dictize as model_dictize
import ckan.plugins.toolkit as tk
import ckan.model as model
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
				lib.helpers.get_action('user_activity_list',{'id':user_dict['id']}) if x['data'].get('package')]
    raw_list = created_datasets_list + active_datasets_list
    filtered_dict = {}
    for dataset in raw_list:
	if dataset['id'] not in filtered_dict.keys():
		filtered_dict[dataset['id']] = dataset
    return filtered_dict.values()

def get_related_dataset(related_id):
    result = model.Session.execute("select dataset_id from related_dataset where related_id =\'"+related_id+"\' limit 1;").first()[0]
    return result

def related_create(context, data_dict=None):
    return {'success': False, 'msg': 'No one is allowed to create related items'}

class DataGovAuPlugin(plugins.SingletonPlugin,
                                tk.DefaultDatasetForm):
    '''An example IDatasetForm CKAN plugin.

    Uses a tag vocabulary to add a custom metadata field to datasets.

    '''
    plugins.implements(plugins.IConfigurer, inherit=False)
    plugins.implements(plugins.ITemplateHelpers, inherit=False)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IActions, inherit=True)

    def get_auth_functions(self):
        return {'related_create': related_create}

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
        return {'get_user_datasets': get_user_datasets, 'get_related_dataset': get_related_dataset}

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