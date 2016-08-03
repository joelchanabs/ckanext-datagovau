import json

from ckan.lib.celery_app import celery

from helpers import log, MSG_ZIP_PREFIX, MSG_SPATIAL_PREFIX
from lib.zip_core import process_zip, zip_delete_all_children
from lib.spatial_core import process_spatial, spatial_delete_all_children


@celery.task(name='datagovau.zip_extract')
def zip_extract(ctx, resource):
    res_dict = json.loads(resource)
    log.info("{0} processing resource {1}".format(MSG_ZIP_PREFIX, res_dict.get('name', res_dict['id'])))
    process_zip(json.loads(ctx), res_dict)


@celery.task(name='datagovau.zip_cleanup')
def zip_cleanup(ctx, resource):
    res_dict = json.loads(resource)
    log.info("{0} deleting children of resource {1}".format(MSG_ZIP_PREFIX, res_dict.get('name', res_dict['id'])))
    zip_delete_all_children(json.loads(ctx), res_dict)


@celery.task(name='datagovau.spatial_ingest')
def spatial_ingest(ctx, resource):
    res_dict = json.loads(resource)
    log.info("{0} processing resource {1}".format(MSG_SPATIAL_PREFIX, res_dict.get('name', res_dict['id'])))
    process_spatial(json.loads(ctx), res_dict)


@celery.task(name='datagovau.spatial_cleanup')
def delete_children_resources(ctx, resource):
    res_dict = json.loads(resource)
    log.info("{0} deleting children for resource {1}".format(MSG_SPATIAL_PREFIX, res_dict.get('name', res_dict['id'])))
    spatial_delete_all_children(json.loads(ctx), res_dict)