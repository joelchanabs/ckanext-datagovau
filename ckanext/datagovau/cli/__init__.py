import click

from .maintain import maintain
from .geoserveringestor import geoserver_ingestor
from .googleanalytics import stats
from .spatialingestor import spatial_ingestor


def get_commands():
    return [spatial_ingestor, dga, geoserver_ingestor]


@click.group(short_help="DGA CLI")
@click.help_option("-h", "--help")
def dga():
    pass


dga.add_command(maintain)
dga.add_command(stats)
