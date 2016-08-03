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


class ZipExtractorPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurable, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.IResourceUrlChange)
    p.implements(p.IDomainObjectModification, inherit=True)
    p.implements(p.ITemplateHelpers, inherit=True)
    p.implements(p.IRoutes, inherit=True)

    legacy_mode = False
    resource_show_action = None

    def configure(self, config):
        self.zip_context = helpers.get_zip_context(config)

    def notify(self, entity, operation=None):
        if not isinstance(entity, model.Resource):
            return

        context = self.zip_context
        resource = entity.as_dict()

        context['model'] = model
        if entity.state == 'deleted' and helpers.is_zip_resource(resource) and resource.get('zip_delete_trigger', 'True') == 'True':
            try:
                log.debug('Deleting children of Zip Extracted resource {0}'.format(resource['id']))
                toolkit.get_action('zipextractor_cleanup')(context, resource)
            except toolkit.ValidationError, e:
                # If zipextractor is offline want to catch error instead
                # of raising otherwise resource save will fail with 500
                log.critical(e)
                pass
        elif (not operation or operation == model.domain_object.DomainObjectOperation.new) and (resource.get('zip_extract', '') == 'True' or (context['auto_process'] and helpers.is_zip_resource(resource))):
            # We have an active resource
            if resource.get('zip_extract', '') == 'True':
                if resource.get('zip_creator', None) is not None:
                    context['user'] = resource['zip_creator']
                try:
                    task = toolkit.get_action('task_status_show')(
                        {'ignore_auth': True}, {
                            'entity_id': resource['id'],
                            'task_type': 'zipextractor',
                            'key': 'zipextractor'}
                    )
                    if task.get('state') == 'pending':
                        # There already is a pending ZipExtractor submission,
                        # skip this one ...
                        log.debug(
                            'Skipping Zip Extractor submission for '
                            'resource {0}'.format(resource['id']))
                        return
                except toolkit.ObjectNotFound:
                    pass

                try:
                    log.debug('Submitting resource {0} to Zip Extractor'.format(resource['id']))
                    toolkit.get_action('zipextractor_submit')(context, resource)
                except toolkit.ValidationError, e:
                    log.error(e)
                    pass
            else:
                # Auto-processing a Zip
                try:
                    dataset = toolkit.get_action('package_show')(context, {
                        'id': resource['package_id'],
                    })
                except Exception, e:
                    log.error(
                        "{0} failed to retrieve package ID: {1} with error {2}, {3}".format(MSG_ZIP_PREFIX,
                                                                                            resource[
                                                                                                'package_id'],
                                                                                            str(e),
                                                                                            MSG_ZIP_SKIP_SUFFIX))
                    return

                log.info("{0} loaded dataset {1}.".format(MSG_ZIP_PREFIX, dataset['name']))

                # Check org, package and last editor blacklists
                blacklist_msg = helpers.check_blacklists(context, dataset)
                if blacklist_msg != '':
                    log.info("{0} {1}, {2}".format(MSG_ZIP_PREFIX, blacklist_msg, MSG_ZIP_SKIP_SUFFIX))
                    return

                # We auto_process zip file by updating the resource, which will re-trigger this method
                resource['zip_extract'] = 'True'
                resource['zip_creator'] = context['user']
                try:
                    toolkit.get_action('resource_update')(context, resource)
                except toolkit.ValidationError, e:
                    log.error(e)
                    return

    def before_map(self, m):
        m.connect(
            'resource_zipextract', '/resource_zipextract/{resource_id}',
            controller='ckanext.datagovau.controller:ResourceZipController',
            action='resource_zipextract', ckan_icon='cloud-upload')
        return m

    def get_actions(self):
        return {'zipextractor_submit': action.zipextractor_submit,
                'zipextractor_cleanup': action.zipextractor_cleanup,
                'zipextractor_hook': action.zipextractor_hook,
                'zipextractor_status': action.zipextractor_status}

    def get_auth_functions(self):
        return {'zipextractor_submit': auth.zipextractor_submit,
                'zipextractor_cleanup': auth.zipextractor_cleanup,
                'zipextractor_status': auth.zipextractor_status}

    def get_helpers(self):
        return {'zipextractor_status': helpers.zipextractor_status,
                'zipextractor_status_description': helpers.status_description,
                'zipextractor_is_zip_resource': helpers.is_zip_resource}


class SpatialIngestorPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurable, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.IResourceUrlChange)
    p.implements(p.IDomainObjectModification, inherit=True)
    p.implements(p.ITemplateHelpers, inherit=True)
    p.implements(p.IRoutes, inherit=True)

    legacy_mode = False
    resource_show_action = None

    def configure(self, config):
        self.spatial_context = helpers.get_spatial_context(config)

    def notify(self, entity, operation=None):
        if not isinstance(entity, model.Resource):
            return

        context = self.spatial_context
        resource = entity.as_dict()
        if resource.get('spatial_parent', '') == 'True' or (context['auto_process'] and helpers.is_spatial_resource(resource)):
            context['model'] = model

            if entity.state == 'deleted' and resource.get('spatial_parent', '') == 'True':
                # Check to see if we have a deleted, zip-extracted resource
                try:
                    log.debug('Deleting children of Spatial Ingested resource {0}'.format(resource['id']))
                    toolkit.get_action('spatialingestor_cleanup')(context, resource)
                except toolkit.ValidationError, e:
                    log.error(e)
                    pass
            elif not operation or operation == model.domain_object.DomainObjectOperation.new:
                if resource.get('spatial_parent', '') == 'True':
                    if resource.get('spatial_creator', None) is not None:
                        context['user'] = resource['spatial_creator']
                    try:
                        task = toolkit.get_action('task_status_show')(
                            {'ignore_auth': True}, {
                                'entity_id': resource['id'],
                                'task_type': 'spatialingestor',
                                'key': 'spatialingestor'}
                        )
                        if task.get('state') == 'pending':
                            # There already is a pending ZipExtractor submission,
                            # skip this one ...
                            log.debug(
                                'Skipping Spatial Ingestor submission for '
                                'resource {0}'.format(resource['id']))
                            return
                    except toolkit.ObjectNotFound:
                        pass

                    try:
                        log.debug('Submitting resource {0} to Spatial Ingestor'.format(resource['id']))
                        toolkit.get_action('spatialingestor_submit')(context, resource)
                    except toolkit.ValidationError, e:
                        log.error(e)
                        pass

                else:
                    try:
                        dataset = toolkit.get_action('package_show')(context, {
                            'id': resource['package_id'],
                        })
                    except Exception, e:
                        log.error(
                            "{0} failed to retrieve package ID: {1} with error {2}, {3}".format(MSG_SPATIAL_PREFIX,
                                                                                                resource[
                                                                                                    'package_id'],
                                                                                                str(e),
                                                                                                MSG_SPATIAL_SKIP_SUFFIX))
                        return

                    log.info("{0} loaded dataset {1}.".format(MSG_SPATIAL_PREFIX, dataset['name']))

                    # Check org, package and last editor blacklists
                    blacklist_msg = helpers.check_blacklists(context, dataset)
                    if blacklist_msg != '':
                        log.info("{0} {1}, {2}".format(MSG_SPATIAL_PREFIX, blacklist_msg, MSG_SPATIAL_SKIP_SUFFIX))
                        return

                    # We auto_process zip file by updating the resource, which will re-trigger this method
                    resource['spatial_parent'] = 'True'
                    resource['spatial_creator'] = context['user']
                    try:
                        toolkit.get_action('resource_update')(
                            context, resource
                        )
                        toolkit.get_action('spatialingestor_submit')(
                            context, resource
                        )
                    except toolkit.ValidationError, e:
                        log.error(e)

    def before_map(self, m):
        m.connect(
            'resource_spatialingest', '/resource_spatialingest/{resource_id}',
            controller='ckanext.datagovau.controller:ResourceSpatialController',
            action='resource_spatialingest', ckan_icon='cloud-upload')
        return m

    def get_actions(self):
        return {'spatialingestor_submit': action.spatialingestor_submit,
                'spatialingestor_cleanup': action.spatialingestor_cleanup,
                'spatialingestor_hook': action.spatialingestor_hook,
                'spatialingestor_status': action.spatialingestor_status}

    def get_auth_functions(self):
        return {'spatialingestor_submit': auth.spatialingestor_submit,
                'spatialingestor_cleanup': auth.spatialingestor_cleanup,
                'spatialingestor_status': auth.spatialingestor_status}

    def get_helpers(self):
        return {'spatialingestor_status': helpers.spatialingestor_status,
                'spatialingestor_status_description': helpers.status_description,
                'spatialingestor_is_spatial_resource': helpers.is_spatial_resource}
