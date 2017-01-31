import ckan.model as model
import ckan.plugins as p
import ckan.plugins.toolkit as toolkit
from ckan.lib.plugins import DefaultOrganizationForm

import ckanext.datagovau.helpers as helpers
import ckanext.datagovau.logic.action as action
import ckanext.datagovau.logic.auth as auth
from ckanext.datagovau.helpers import log, MSG_SPATIAL_PREFIX, MSG_SPATIAL_SKIP_SUFFIX, MSG_ZIP_PREFIX, \
    MSG_ZIP_SKIP_SUFFIX

class DataGovAuPlugin(p.SingletonPlugin,
                      toolkit.DefaultDatasetForm):
    '''An example IDatasetForm CKAN plugin.

    Uses a tag vocabulary to add a custom metadata field to datasets.

    '''
    p.implements(p.IConfigurer, inherit=False)
    p.implements(p.ITemplateHelpers, inherit=False)
    p.implements(p.IActions, inherit=True)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.IFacets, inherit=True)

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

        toolkit.add_template_directory(config, 'templates')
        toolkit.add_public_directory(config, 'theme/public')
        toolkit.add_resource('theme/public', 'ckanext-datagovau')
        toolkit.add_resource('public/scripts/vendor/jstree', 'jstree')

    def get_helpers(self):
        return {'get_user_datasets': helpers.get_user_datasets,
                'get_user_datasets_public': helpers.get_user_datasets_public,
                'get_ddg_site_statistics': helpers.get_ddg_site_statistics,
                'get_resource_file_size': helpers.get_resource_file_size,
                'blogfeed': helpers.blogfeed}

    # IActions

    def get_actions(self):
        return {'group_tree': action.group_tree,
                'group_tree_section': action.group_tree_section}


class HierarchyForm(p.SingletonPlugin, DefaultOrganizationForm):
    p.implements(p.IGroupForm, inherit=True)

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