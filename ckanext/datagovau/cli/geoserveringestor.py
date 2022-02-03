import click
from ckan import model
from ckanext.datagovau.geoserver_utils import run_ingestor, CONFIG_PUBLIC_URL
import ckan.plugins.toolkit as tk
import logging


log = logging.getLogger(__name__)

@click.group("geoserver-ingestor", short_help="Ingest spatial data")
@click.help_option("-h", "--help")
def geoserver_ingestor():
    pass

@geoserver_ingestor.command('ingest')
@click.option('-d', '--dataset', help='Get specific dataset', default=False)
@click.option('-o', '--organization', help='Datasets of specific organization', default=False)
def geo_ingest(dataset, organization):
    log.info('Start ingestor script')
    
    log.info('Query Dataset based on script options')
    query = model.Session.query(model.Package).filter_by(state="active")
    if organization:
        query = query.filter(model.Package.owner_org == organization)
    elif dataset:
        query = query.filter(
            (model.Package.name == dataset) | (model.Package.id == dataset)
        )
    
    log.info('Jump into Dataset loop')
    for dataset in query:
        run_ingestor(dataset.id)


# ONE TIME SCRIPT
@geoserver_ingestor.command('rmv-old-geo-res')
def rmv_old_geo_res():
    geo_resources = model.Session.query(model.Resource)\
        .filter(model.Resource.url.ilike(CONFIG_PUBLIC_URL +'%'))
    length = len(geo_resources.all())
    log.debug(f"Found {length} resources...")
    for res in geo_resources:
        log.debug(f"Removing {res.id} resource.")
        tk.get_action("resource_delete")(
            {"user": tk.config.get(
                "ckanext.datagovau.spatialingestor.username", ''),
                "ignore_auth": True
            },
            {
                'id': res.id
            }
        )
