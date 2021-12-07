from __future__ import annotations

import logging

from typing import Iterable, Optional

import click
import ckanapi

from ..utils import temp_dir

log = logging.getLogger(__name__)


@click.group()
@click.help_option("-h", "--help")
def maintain():
    """Maintenance tasks"""
    pass


@maintain.command()
@click.argument("ids", nargs=-1)
@click.option("-u", "--username", help="CKAN user who performs extraction.")
@click.option(
    "--tmp-dir", default="/tmp", help="Root folder for temporal files"
)
@click.option(
    "--days-to-buffer",
    "days",
    default=3,
    type=int,
    help="Extract datasets modified up to <days> ago",
)
@click.option(
    "--skip-errors",
    is_flag=True,
    help="Do not interrupt extraction even after an error",
)
@click.help_option("-h", "--help")
@click.pass_context
def zip_extract(
    ctx: click.Context,
    ids: Iterable[str],
    tmp_dir: str,
    username: Optional[str],
    days: int,
    skip_errors: bool,
):
    """ZIP extractor for data.gov.au"""
    ckan = ckanapi.LocalCKAN(username)
    from . import _zip_extract as z

    if not ids:
        ids = z.get_dataset_ids(ckan, days)
    with ctx.meta["flask_app"].test_request_context():
        for resource, dataset in z.select_extractable_resources(ckan, ids):
            with temp_dir(resource["id"], tmp_dir) as path:
                for result in z.extract_resource(resource, path):
                    try:
                        z.update_resource(*result, ckan, resource, dataset)
                    except ckanapi.ValidationError:
                        log.error(
                            "Cannot update resource {} from dataset {}".format(
                                resource["id"], dataset["id"]
                            )
                        )
                        if skip_errors:
                            continue
                        raise
