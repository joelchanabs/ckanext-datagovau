import ckan.lib.base as base
import ckan.logic as logic
import ckan.model as model
import ckan.plugins.toolkit as toolkit
from ckan.common import _

from ckanext.datagovau.helpers import get_zip_context, log

class ResourceZipController(base.BaseController):
    def resource_zipextract(self, resource_id):

        zip_context = get_zip_context()

        try:
            if toolkit.c.user not in ['', None]:
                zip_context['user'] = toolkit.c.user
        except:
            pass

        resource = toolkit.get_action('resource_show')(
            zip_context, {'id': resource_id}
        )

        if toolkit.request.method == 'POST':
            try:
                if resource.get('zip_extract', '') != 'True' or resource.get('zip_creator', "") != zip_context['user']:
                    resource['zip_extract'] = 'True'
                    resource['zip_creator'] = zip_context['user']
                    toolkit.get_action('resource_update')(
                        zip_context, resource
                    )

                toolkit.get_action('zipextractor_submit')(
                    zip_context, resource
                )
            except logic.ValidationError:
                pass

        try:
            toolkit.c.pkg_dict = toolkit.get_action('package_show')(
                zip_context, {'id': resource['package_id']}
            )

            toolkit.c.resource = resource
        except logic.NotFound:
            base.abort(404, _('Resource not found'))
        except logic.NotAuthorized:
            base.abort(401, _('Unauthorized to edit this resource'))

        try:
            zipextractor_status = toolkit.get_action('zipextractor_status')(zip_context, resource)
        except logic.NotFound:
            zipextractor_status = {}
        except logic.NotAuthorized:
            base.abort(401, _('Not authorized to see this page'))

        return base.render('package/resource_zipextract.html',
                           extra_vars={'status': zipextractor_status})


class ResourceSpatialController(base.BaseController):
    def resource_spatialingest(self, resource_id):

        spatial_context = get_zip_context()

        try:
            if toolkit.c.user not in ['', None]:
                spatial_context['user'] = toolkit.c.user
        except:
            pass

        resource = toolkit.get_action('resource_show')(
            spatial_context, {'id': resource_id}
        )

        if toolkit.request.method == 'POST':
            try:
                if resource.get('spatial_parent', '') != 'True' or resource.get('spatial_creator', "") != spatial_context['user']:
                    resource['spatial_parent'] = 'True'
                    resource['spatial_creator'] = spatial_context['user']
                    toolkit.get_action('resource_update')(
                        spatial_context, resource
                    )

                toolkit.get_action('spatialingestor_submit')(
                    spatial_context, resource
                )
            except logic.ValidationError:
                pass

        try:
            toolkit.c.pkg_dict = toolkit.get_action('package_show')(
                spatial_context, {'id': resource['package_id']}
            )

            toolkit.c.resource = resource
        except logic.NotFound:
            base.abort(404, _('Resource not found'))
        except logic.NotAuthorized:
            base.abort(401, _('Unauthorized to edit this resource'))

        try:
            spatialingestor_status = toolkit.get_action('spatialingestor_status')(spatial_context, resource)
        except logic.NotFound:
            spatialingestor_status = {}
        except logic.NotAuthorized:
            base.abort(401, _('Not authorized to see this page'))

        return base.render('package/resource_spatialingest.html',
                           extra_vars={'status': spatialingestor_status})
