from ckan.lib.celery_app import celery
from lib import process_spatial, process_zip, delete_all_children, MSG_SPATIAL_PREFIX, MSG_ZIP_PREFIX
import logging

log = logging.getLogger('ckanext_datagovau')

@celery.task(name='datagovau.spatial_ingest')
def spatial_ingest(ctx, resource):
    log.info("{0} processing resource {1}".format(MSG_SPATIAL_PREFIX, resource['id']))
    process_spatial(ctx, resource)

@celery.task(name='datagovau.zip_extract')
def zip_extract(ctx, resource):
    log.info("{0} processing resource {1}".format(MSG_ZIP_PREFIX, resource['id']))
    process_zip(ctx, resource)

@celery.task(name='datagovau.delete_children')
def delete_children_resources(ctx, resource):
    log.info("Deleting children for resource {0}".format(resource['id']))
    delete_all_children(ctx, resource)