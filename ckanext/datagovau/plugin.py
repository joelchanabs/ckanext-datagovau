import logging

import ckan.plugins as plugins
import ckan.lib as lib
import ckan.lib.dictization.model_dictize as model_dictize
import ckan.plugins.toolkit as tk
import ckan.model as model
from pylons import config

from sqlalchemy import orm
import ckan.model

#parse the activity feed for last active non-system user
def get_last_active_user(id):
    system_user = lib.helpers.get_action('user_show',{'id': config.get('ckan.site_id', 'ckan_site_user')})
    user_list = [x for x in lib.helpers.get_action('package_activity_list',{'id':id}) if x['user_id'] != system_user['id']]
    user = None
    if len(user_list) > 0:
    	user = user_list[0].get('user_id', None)
    if user is None:
	return system_user
    else:
	return lib.helpers.get_action('user_show',{'id':user})

# get user created datasets and those they have edited
def get_user_datasets(user_dict):
    created_datasets_list = user_dict['datasets']
    active_datasets_list = [x['data']['package'] for x in 
				lib.helpers.get_action('user_activity_list',{'id':user_dict['id']}) if x['data'].get('package')]
    return created_datasets_list + active_datasets_list

def get_dga_stats():
	connection = model.Session.connection()
        res = connection.execute("SELECT 'organization', count(*) from \"group\" where type = 'organization' and state = 'active' 		union select 'package', count(*) from package where state='active' or state='draft' or state='draft-complete' 		union select 'resource', count(*) from resource where state='active' 		union select name||role, 0 from user_object_role inner join \"user\" on user_object_role.user_id = \"user\".id where name not in ('logged_in','visitor') group by name,role;")
        return res


def get_activity_counts():
	connection = model.Session.connection()
        res = connection.execute("select to_char(timestamp, 'YYYY-MM') as month,activity_type, count(*) from activity group by month, activity_type order by month;").fetchall();
        return res


class DataGovAuPlugin(plugins.SingletonPlugin,
                                tk.DefaultDatasetForm):
    '''An example IDatasetForm CKAN plugin.

    Uses a tag vocabulary to add a custom metadata field to datasets.

    '''
    plugins.implements(plugins.IConfigurer, inherit=False)
    plugins.implements(plugins.IDatasetForm, inherit=False)
    plugins.implements(plugins.ITemplateHelpers, inherit=False)

    def update_config(self, config):
        # Add this plugin's templates dir to CKAN's extra_template_paths, so
        # that CKAN will use this plugin's custom templates.
        # here = os.path.dirname(__file__)
        # rootdir = os.path.dirname(os.path.dirname(here))

        tk.add_template_directory(config, 'templates')
        tk.add_public_directory(config, 'theme/public')
        tk.add_resource('theme/public', 'ckanext-datagovau')
        # config['licenses_group_url'] = 'http://%(ckan.site_url)/licenses.json'

    def get_helpers(self):
        return {'get_last_active_user': get_last_active_user, 'get_user_datasets': get_user_datasets, 'get_dga_stats': get_dga_stats, 'get_activity_counts': get_activity_counts}

    def is_fallback(self):
        # Return True to register this plugin as the default handler for
        # package types not handled by any other IDatasetForm plugin.
        return True

    def package_types(self):
        # This plugin doesn't handle any special package types, it just
        # registers itself as the default (above).
        return []


    def create_package_schema(self):
        schema = super(DataGovAuPlugin, self).create_package_schema()
        schema = self._modify_package_schema(schema)
        return schema

    def update_package_schema(self):
        schema = super(DataGovAuPlugin, self).update_package_schema()
        schema = self._modify_package_schema(schema)
        return schema

    def show_package_schema(self):
        schema = super(DataGovAuPlugin, self).show_package_schema()

        # Don't show vocab tags mixed in with normal 'free' tags
        # (e.g. on dataset pages, or on the search page)
        schema['tags']['__extras'].append(tk.get_converter('free_tags_only'))

        # Add our custom_text field to the dataset schema.
        # ignore_missing == optional
        # ignore_empty == mandatory but not for viewing
        # !!! always convert_from_extras first
        schema.update({
            'agency_program': [tk.get_converter('convert_from_extras'),
                               tk.get_validator('ignore_missing')],
            'contact_point': [tk.get_converter('convert_from_extras'),
                              tk.get_validator('ignore_empty')],
            'spatial_coverage': [tk.get_converter('convert_from_extras'),
                                 tk.get_validator('ignore_empty')],
            'granularity': [tk.get_converter('convert_from_extras'),
                            tk.get_validator('ignore_empty')],
            'jurisdiction': [tk.get_converter('convert_from_extras'),
                             tk.get_validator('ignore_empty')],
            'temporal_coverage': [tk.get_converter('convert_from_extras'),
                                  tk.get_validator('ignore_empty')],
            'data_state': [tk.get_converter('convert_from_extras'),
                           tk.get_validator('ignore_empty')],
            'update_freq': [tk.get_converter('convert_from_extras'),
                            tk.get_validator('ignore_empty')]
        })
        return schema

    def _modify_package_schema(self, schema):
        # Add our custom_test metadata field to the schema, this one will use
        # convert_to_extras instead of convert_to_tags.
        # ignore_missing == optional
        # not_empty == mandatory, enforced here while modifying

        schema.update({
            'agency_program': [tk.get_validator('ignore_missing'),
                               tk.get_converter('convert_to_extras')],
            'contact_point': [tk.get_converter('convert_to_extras'),
                              tk.get_validator('not_empty')],
            'spatial_coverage': [tk.get_converter('convert_to_extras'),
                                 tk.get_validator('not_empty')],
            'granularity': [tk.get_converter('convert_to_extras'),
                            tk.get_validator('not_empty')],
            'jurisdiction': [tk.get_converter('convert_to_extras'),
                             tk.get_validator('not_empty')],
            'temporal_coverage': [tk.get_converter('convert_to_extras'),
                                  tk.get_validator('not_empty')],
            'data_state': [tk.get_converter('convert_to_extras'),
                           tk.get_validator('not_empty')],
            'update_freq': [tk.get_converter('convert_to_extras'),
                            tk.get_validator('not_empty')]
        })
        return schema

    # These methods just record how many times they're called, for testing
    # purposes.
    # TODO: It might be better to test that custom templates returned by
    # these methods are actually used, not just that the methods get
    # called.

    def setup_template_variables(self, context, data_dict):
        return super(DataGovAuPlugin, self).setup_template_variables(
            context, data_dict)

    def new_template(self):
        return super(DataGovAuPlugin, self).new_template()

    def read_template(self):
        return super(DataGovAuPlugin, self).read_template()

    def edit_template(self):
        return super(DataGovAuPlugin, self).edit_template()

    def search_template(self):
        return super(DataGovAuPlugin, self).search_template()

    def history_template(self):
        return super(DataGovAuPlugin, self).history_template()

    def package_form(self):
        return super(DataGovAuPlugin, self).package_form()

